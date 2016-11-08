// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"fmt"
	"io"

	"strings"

	"github.com/bytedance/dbatman/database/cluster"
	"github.com/bytedance/dbatman/database/mysql"
	"github.com/bytedance/dbatman/database/sql/driver"
	"github.com/bytedance/dbatman/hack"
	"github.com/ngaut/log"
)

func (session *Session) dispatch(data []byte) (err error) {
	logLevel := 3
	cmd := data[0]
	data = data[1:]
	defer func() {
		flush_error := session.fc.Flush()
		if err == nil {
			err = flush_error
		}
	}()

	switch cmd {
	case mysql.ComQuery:
		err = session.comQuery(hack.String(data))
	case mysql.ComPing:
		err = session.fc.WriteOK(nil)
	case mysql.ComInitDB:
		if err := session.useDB(hack.String(data)); err != nil {
			err = session.handleMySQLError(err)
		} else {
			err = session.fc.WriteOK(nil)
		}
	case mysql.ComFieldList:
		err = session.handleFieldList(data)
	case mysql.ComStmtPrepare:
		err = session.handleComStmtPrepare(string(data))
	case mysql.ComStmtExecute:
		err = session.handleComStmtExecute(data)
	case mysql.ComStmtClose:
		err = session.handleComStmtClose(data)
	case mysql.ComStmtSendLongData:
		err = session.handleComStmtSendLongData(data)
	case mysql.ComStmtReset:
		err = session.handleComStmtReset(data)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		log.Warnf(msg)
		err = mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR, msg)
	}
	//write back mertics
	if err != nil {
		logLevel = 6
	}
	cluster.DoDbMertics(logLevel, session.DbName)

	return
}

func proceDbName(db string) string {
	ret := db
	// filter the `` of the `db`
	if strings.Contains(db, "`") {
		log.Debug("db name error :,", db)
		a := strings.Split(db, "`")
		ret = a[1]
	}
	return ret

}
func (session *Session) useDB(dbName string) error {
	// log.Info("use db: ", dbName)
	// log.Info("transfer db", proceDbName(db))
	db := proceDbName(dbName)
	session.DbName = dbName
	if session.cluster != nil {
		if session.cluster.DBName != db {
			// log.Debug("er1,:", session.cluster.DBName)
			return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, db)
		}

		return nil
	}

	if _, err := session.config.GetClusterByDBName(db); err != nil {
		// log.Debug("er2,:", err)
		return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, db)
	} else if session.cluster, err = cluster.New(session.user.ClusterName); err != nil {
		// log.Debug("er3,:", err)
		return err
	}

	if session.bc == nil {
		master, err := session.cluster.Master()
		if err != nil {
			// log.Debug("er3,:", err)
			return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, db)
		}
		slave, err := session.cluster.Slave()
		if err != nil {
			slave = master
		}
		session.bc = &SqlConn{
			master:  master,
			slave:   slave,
			stmts:   make(map[uint32]*mysql.Stmt),
			tx:      nil,
			session: session,
		}
	}

	return nil
}

func (session *Session) IsAutoCommit() bool {
	return session.fc.Status()&uint16(mysql.StatusInAutocommit) > 0
}

func (session *Session) writeRows(rs mysql.Rows) error {
	var cols []driver.RawPacket
	var err error
	cols, err = rs.ColumnPackets()

	if err != nil {
		return session.handleMySQLError(err)
	}

	// Send a packet contains column length
	data := make([]byte, 4, 32)
	data = mysql.AppendLengthEncodedInteger(data, uint64(len(cols)))
	if err = session.fc.WritePacket(data); err != nil {
		return err
	}

	// Write Columns Packet
	for _, col := range cols {
		if err := session.fc.WritePacket(col); err != nil {
			log.Debugf("write columns packet error %v", err)
			return err
		}
	}

	// TODO Write a ok packet
	if err = session.fc.WriteEOF(); err != nil {
		return err
	}

	for {
		packet, err := rs.NextRowPacket()
		// var p []byte = packet
		// defer mysql.SysBytePool.Return([]byte(packet))

		// Handle Error

		//warnging if in cli_deprecate_mode will get a ok_packet
		if err != nil {
			if err == io.EOF {
				return session.fc.WriteEOF()
			} else {
				return session.handleMySQLError(err)
			}
		}

		if err := session.fc.WritePacket(packet); err != nil {
			return err
		}
	}

	return nil
}

func (session *Session) handleMySQLError(e error) error {
	cluster.DoDbMertics(5, session.DbName)
	switch inst := e.(type) {
	case *mysql.MySQLError:
		session.fc.WriteError(inst)
		return nil
	default:
		log.Warnf("default error: %T %s", e, e)
		return e
	}
}
