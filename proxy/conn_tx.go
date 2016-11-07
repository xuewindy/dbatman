package proxy

import (
	"errors"

	. "github.com/bytedance/dbatman/database/mysql"
)

func (c *Session) isInTransaction() bool {
	return c.fc.Status()&uint16(StatusInTrans) > 0
}

func (c *Session) isAutoCommit() bool {
	return c.fc.Status()&uint16(StatusInAutocommit) > 0
}

func (c *Session) handleBegin() error {

	// We already in transaction
	// if not in default tx isolation
	// pass the status to the driver layer
	if c.isInTransaction() {
		return c.fc.WriteOK(nil)
	}

	c.fc.XORStatus(uint16(StatusInTrans))
	if err := c.bc.begin(c); err != nil {
		return c.handleMySQLError(err)
	}

	return c.fc.WriteOK(nil)
}

func (c *Session) handleCommit() (err error) {

	if !c.isInTransaction() {
		return c.fc.WriteOK(nil)
	}

	defer func() {
		if c.isInTransaction() {
			if c.isAutoCommit() {
				c.fc.AndStatus(uint16(^StatusInTrans))
				// fmt.Println("close the proxy tx")
			}
		}
	}()

	if err := c.bc.commit(c.isAutoCommit()); err != nil {
		return c.handleMySQLError(err)
	} else {
		return c.fc.WriteOK(nil)
	}
}

//this func clear the proxy -> db conn autocommit status
//dont need to communicate with front client
func (c *Session) handleClearAutoCommit() error {
	if !c.isInTransaction() {
		// return c.fc.WriteOK(nil)
		return errors.New("autocommit status err,not in transaction status")

	}
	if c.isAutoCommit() {
		return errors.New("autocommit status err,not in autocommit status")

	}
	if err := c.bc.clearAutoCommit(); err != nil {
		// return c.handleMySQLError(err)
		return err
	}
	return nil
}
func (c *Session) handleRollback() (err error) {
	if !c.isInTransaction() {
		return c.fc.WriteOK(nil)
	}

	defer func() {
		if c.isInTransaction() {
			if c.isAutoCommit() {
				c.fc.AndStatus(uint16(^StatusInTrans))
				// fmt.Println("close the proxy tx")
			}
		}
	}()
	// fmt.Println("rollback")
	// fmt.Println("this is a autocommit tx:", !c.isAutoCommit())
	if err := c.bc.rollback(c.isAutoCommit()); err != nil {
		return c.handleMySQLError(err)
	}

	return c.fc.WriteOK(nil)
}
