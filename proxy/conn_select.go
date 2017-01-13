package proxy

import "github.com/bytedance/dbatman/sqlparser"

func (session *Session) handleQuery(stmt sqlparser.Statement, sqlstmt string) error {

	// if err := session.checkDB(stmt); err != nil {
	// 	log.Debugf("check db error: %s", err.Error())
	// 	return err
	// }
	isread := false

	// if s, ok := stmt.(sqlparser.Select); ok {
	// 	isread = !s.IsLocked()
	// } else if _, sok := stmt.(sqlparser.Show); sok {
	// 	isread = true
	// }

	rs, err := session.Executor(isread).Query(sqlstmt)
	// TODO here should handler error
	if err != nil {
		return session.handleMySQLError(err)
	}

	defer rs.Close()
	return session.writeRows(rs)
}
