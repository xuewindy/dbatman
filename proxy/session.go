// Copyright 2016 ByteDance, Inc.
//
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
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/bytedance/dbatman/cmd/version"
	"github.com/bytedance/dbatman/config"
	"github.com/bytedance/dbatman/database/cluster"
	. "github.com/bytedance/dbatman/database/mysql"
	"github.com/bytedance/dbatman/database/sql/driver"
	"github.com/bytedance/dbatman/hack"
	"github.com/ngaut/log"
)

type Session struct {
	server *Server
	config *config.ProxyConfig
	user   *config.UserConfig

	salt []byte

	cluster *cluster.Cluster
	bc      *SqlConn
	fc      *MySQLServerConn

	cliAddr    string //client ip for auth
	autoCommit uint
	sessionId  int64

	//session status
	txIsolationStmt  string
	txIsolationInDef bool //is the tx isolation level in dafault?

	closed bool

	// lastcmd uint8
}

var errSessionQuit error = errors.New("session closed by client")

func (s *Server) newSession(conn net.Conn) *Session {
	session := new(Session)
	id := s.GetSessionId()
	session.server = s
	session.config = s.cfg.GetConfig()
	session.salt, _ = RandomBuf(20)
	session.autoCommit = 0
	session.cliAddr = strings.Split(conn.RemoteAddr().String(), ":")[0]
	session.sessionId = id
	session.txIsolationInDef = true
	session.fc = NewMySQLServerConn(session, conn)
	//session.lastcmd = ComQuit
	log.Info("start new session", session.sessionId)
	return session
}

func (session *Session) Handshake() error {

	if err := session.fc.Handshake(); err != nil {
		erro := fmt.Errorf("session %d : handshake error: %s", session.sessionId, err.Error())
		return erro
	}

	return nil
}

func (session *Session) Run() error {

	for {

		data, err := session.fc.ReadPacket()

		if err != nil {
			// log.Warn(err)
			// Usually client close the conn
			return err
		}

		if data[0] == ComQuit {
			return errSessionQuit
		}

		if err := session.dispatch(data); err != nil {
			if err == driver.ErrBadConn {
				// TODO handle error
			}

			log.Warnf("sessionId %d:dispatch error: %s", session.sessionId, err.Error())
			// session.fc.WriteError(err)
			return err
		}

		session.fc.ResetSequence()

		if session.closed {
			// TODO return MySQL Go Away ?
			return errors.New("session closed!")
		}
	}

	return nil
}

func (session *Session) Close() error {
	if session.closed {
		return nil
	}

	//current connection is in AC tx mode reset before store in poll
	if !session.isAutoCommit() {
		//Debug
		if !session.isInTransaction() {
			err := errors.New("transaction must be in true in the autocommit = 0 mode")
			return err
		}
		//rollback uncommit data

		//set the autocommit mdoe as true
		session.clearAutoCommitTx()
		for _, s := range session.bc.stmts {
			s.Close()
		}

	}
	if session.isInTransaction() {
		// session.handleCommit()
		log.Debugf("session : %d reset the  tx status", session.sessionId)
		if session.txIsolationInDef == false {
			session.bc.tx.Exec("set session transaction isolation level read uncommitted;") //reset to default level
		}
		if err := session.bc.rollback(session.isAutoCommit()); err != nil {
			log.Info(err.Error)
		}
	}
	session.fc.Close()

	// session.bc.tx.Exec("set autocommit =0 ")
	// TODO transaction
	//	session.rollback()

	// TODO stmts
	// for _, s := range session.stmts {
	// 	s.Close()
	// }

	// session.stmts = nil

	session.closed = true

	return nil
}

func (session *Session) ServerName() []byte {
	return hack.Slice(version.Version)
}
func (session *Session) GetIsoLevel() (string, bool) {
	if session.txIsolationInDef {
		sql := "set session transaction isolation level read committed"
		return sql, true
	} else {
		return session.txIsolationStmt, false
	}
}

func (session *Session) Salt() []byte {
	return session.salt
}
