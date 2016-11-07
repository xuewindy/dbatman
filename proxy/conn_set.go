package proxy

import (
	"fmt"

	"strings"

	. "github.com/bytedance/dbatman/database/mysql"
	"github.com/bytedance/dbatman/parser"
	"github.com/ngaut/log"
)

func (c *Session) handleSet(stmt *parser.Set, sql string) error {
	if len(stmt.VarList) < 1 {
		return fmt.Errorf("must set one item at least")
	}

	var err error
	for _, v := range stmt.VarList {
		if strings.ToUpper(v.Name) == "AUTOCOMMIT" {
			log.Debug("handle autocommit")
			err = c.handleSetAutoCommit(v.Value) //??
		}
	}

	if err != nil {
		return err
	}

	defer func() {
		//only execute when the autocommit 0->1 //clear
		if c.autoCommit == 1 {
			log.Debug("clear autocommit tx")
			c.clearAutoCommitTx()
		}

	}()
	return c.handleOtherSet(stmt, sql)
}

func (c *Session) clearAutoCommitTx() {
	// clear the AUTOCOMMIT status
	var err error
	if c.bc == nil || c.bc.tx == nil {
		log.Warnf("sessionid %d : clear autcomit err,for nil pointer", c.sessionId)
		return
	}
	err = c.handleClearAutoCommit()
	if err != nil {
		log.Warnf("session id :%d,clear autocommit err for %s", c.sessionId, err)
	}

	// _, err = c.bc.tx.Exec("set autocommit = 1")
	// don;t need to put conn back
	// if err != nil {
	// log.Warnf("session id :%d,clear autocommit err for %s", c.sessionId, err)
	// }
	//put back the conn
	// c.bc.tx.PutConn(err)
	// c.bc.tx = nil

	c.fc.XORStatus(uint16(StatusInAutocommit))
	c.fc.AndStatus(^uint16(StatusInTrans))
	c.autoCommit = 0
}

func (c *Session) handleSetAutoCommit(val parser.IExpr) error {

	var stmt *parser.Predicate
	var ok bool
	if stmt, ok = val.(*parser.Predicate); !ok {
		return fmt.Errorf("set autocommit is not support for complicate expressions")
	}

	switch value := stmt.Expr.(type) {
	case parser.BoolVal:
		//same as NumVal
		// var a bool = bool(parser.BoolVal)

		// var b bool = true
		if value == true {
			//
			if c.isAutoCommit() {
				return nil
			}

			//inply the tx  cleanUp step after last query c.handleOtherSet(stmt, sql)
			c.autoCommit = 1 //indicate 0 -> 1
			//TODO when previous handle error need

			log.Debug("autocommit is set")
		} else {
			// indicate a transection
			//current is autocommit = true  do nothing
			if !c.isAutoCommit() {
				return nil
			}
			c.fc.AndStatus(^uint16(StatusInAutocommit))
			////atuocommit  1->0 start a transection
			err := c.bc.begin(c)
			if err != nil {
				log.Debug(err)
				c.fc.XORStatus(uint16(StatusInAutocommit))
				return nil
			}
			c.fc.XORStatus(uint16(StatusInTrans))
			c.autoCommit = 2 // indicate 1 -> zero
			// log.Debug("start a transection")
			// log.Debug("auto commit is unset")

		}
	case parser.NumVal:
		if i, err := value.ParseInt(); err != nil {
			return err
		} else if i == 1 {
			//
			if c.isAutoCommit() {
				return nil
			}

			//inply the tx  cleanUp step after last query c.handleOtherSet(stmt, sql)
			c.autoCommit = 1 //indicate 0 -> 1
			//TODO when previous handle error need

			log.Debug("autocommit is set")
		} else if i == 0 {
			// indicate a transection
			//current is autocommit = true  do nothing
			if !c.isAutoCommit() {
				return nil
			}
			c.fc.AndStatus(^uint16(StatusInAutocommit))
			////atuocommit  1->0 start a transection
			err := c.bc.begin(c)
			if err != nil {
				log.Debug(err)
				c.fc.XORStatus(uint16(StatusInAutocommit))
				return nil
			}
			c.fc.XORStatus(uint16(StatusInTrans))
			c.autoCommit = 2 // indicate 1 -> zero
			// log.Debug("start a transection")
			// log.Debug("auto commit is unset")
		} else {
			return fmt.Errorf("Variable 'autocommit' can't be set to the value of '%s'", i)
		}
	case parser.StrVal:
		if s := value.Trim(); s == "" {
			return fmt.Errorf("Variable 'autocommit' can't be set to the value of ''")
		} else if us := strings.ToUpper(s); us == `ON` {
			c.fc.XORStatus(uint16(StatusInAutocommit))
			log.Debug("auto commit is set")
			// return c.handleBegin()
		} else if us == `OFF` {
			c.fc.AndStatus(^uint16(StatusInAutocommit))
			log.Debug("auto commit is unset")
		} else {
			return fmt.Errorf("Variable 'autocommit' can't be set to the value of '%s'", us)
		}
	default:
		// fmt.Println()
		return fmt.Errorf("set autocommit error, value type is %T", val, value)
	}

	return nil
}

func (c *Session) handleSetNames(val parser.IValExpr) error {
	value, ok := val.(parser.StrVal)
	if !ok {
		return fmt.Errorf("set names charset error")
	}

	charset := strings.ToLower(string(value))
	cid, ok := CharsetIds[charset]
	if !ok {
		return fmt.Errorf("invalid charset %s", charset)
	}

	c.fc.SetCollation(cid)

	return c.fc.WriteOK(nil)
}

func (c *Session) handleOtherSet(stmt parser.IStatement, sql string) error {
	return c.handleExec(stmt, sql, false)
}
