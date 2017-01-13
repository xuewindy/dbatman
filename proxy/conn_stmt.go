package proxy

// func (c *Session) handleComStmtPrepare(sqlstmt string) error {
// 	stmt, err := sqlparser.Parse(sqlstmt)
// 	log.Infof("session %d: %s", c.sessionId, sqlstmt)
// 	if err != nil {

// 		log.Warningf(`parse sql "%s" error "%s"`, sqlstmt, err.Error())
// 		return c.handleMySQLError(
// 			mysql.NewDefaultError(mysql.ER_SYNTAX_ERROR, err.Error()))
// 	}

// 	// Only a few statements supported by prepare statements
// 	// http://dev.mysql.com/worklog/task/?id=2871
// 	switch v := stmt.(type) {
// 	case sqlparser.Select, *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete,
// 		*sqlparser.Replace,
// 		sqlparser.IDDLStatement,
// 		*sqlparser.ShowTables,
// 		*sqlparser.ShowColumns,
// 		*sqlparser.ShowVariables,
// 		*sqlparser.ShowIndex,
// 		*sqlparser.Set,
// 		*sqlparser.DescribeTable,
// 		*sqlparser.Do:
// 		return c.prepare(v, sqlstmt)
// 	default:
// 		log.Warnf("session %d :statement %T[%s] not support prepare ops", c.sessionId, stmt, sqlstmt)
// 		return c.handleMySQLError(
// 			mysql.NewDefaultError(mysql.ER_UNSUPPORTED_PS))
// 	}
// }

// func (session *Session) prepare(istmt sqlparser.IStatement, sqlstmt string) error {
// 	if err := session.checkDB(istmt); err != nil {
// 		log.Debugf("check db error: %s", err.Error())
// 		return err
// 	}

// 	isread := false

// 	if s, ok := istmt.(sqlparser.ISelect); ok {
// 		isread = !s.IsLocked()
// 	}

// 	if session.isInTransaction() || !session.isAutoCommit() {
// 		isread = false
// 	}

// 	stmt, err := session.Executor(isread).Prepare(sqlstmt)
// 	// TODO here should handler error
// 	if err != nil {
// 		return session.handleMySQLError(err)
// 	}

// 	//	record the sql
// 	stmt.SQL = istmt

// 	// TODO duplicate
// 	session.bc.stmts[stmt.ID] = stmt

// 	return session.writePrepareResult(stmt)
// }

// func (session *Session) writePrepareResult(stmt *mysql.Stmt) error {

// 	colen := len(stmt.Columns)
// 	paramlen := len(stmt.Params)

// 	// Prepare Header
// 	header := make([]byte, mysql.PacketHeaderLen, 12+mysql.PacketHeaderLen)

// 	// OK Status
// 	header = append(header, 0)
// 	header = append(header, byte(stmt.ID), byte(stmt.ID>>8), byte(stmt.ID>>16), byte(stmt.ID>>24))

// 	header = append(header, byte(colen), byte(colen>>8))
// 	header = append(header, byte(paramlen), byte(paramlen>>8))

// 	// reserved 00
// 	header = append(header, 0)

// 	// warning count 00
// 	// TODO
// 	header = append(header, 0, 0)

// 	if err := session.fc.WritePacket(header); err != nil {
// 		return session.handleMySQLError(err)
// 	}

// 	if paramlen > 0 {
// 		for _, p := range stmt.Params {
// 			if err := session.fc.WritePacket(p); err != nil {
// 				return session.handleMySQLError(err)
// 			}
// 		}

// 		if err := session.fc.WriteEOF(); err != nil {
// 			return session.handleMySQLError(err)
// 		}

// 	}

// 	if colen > 0 {
// 		for _, c := range stmt.Columns {
// 			if err := session.fc.WritePacket(c); err != nil {
// 				return session.handleMySQLError(err)
// 			}
// 		}

// 		if err := session.fc.WriteEOF(); err != nil {
// 			return session.handleMySQLError(err)
// 		}
// 	}

// 	return nil
// }

// func (session *Session) handleComStmtExecute(data []byte) error {

// 	if len(data) < 9 {
// 		return session.handleMySQLError(mysql.ErrMalformPkt)
// 	}

// 	pos := 0
// 	id := binary.LittleEndian.Uint32(data[0:4])
// 	pos += 4

// 	stmt, ok := session.bc.stmts[id]
// 	if !ok {
// 		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
// 			strconv.FormatUint(uint64(id), 10), "stmt_execute")
// 	}

// 	flag := data[pos]
// 	pos++

// 	//now we only support CURSOR_TYPE_NO_CURSOR flag
// 	if flag != 0 {
// 		return mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("unsupported flag %d", flag))
// 	}

// 	//skip iteration-count, always 1
// 	pos += 4

// 	var err error
// 	switch stmt.SQL.(type) {
// 	case sqlparser.ISelect,
// 		*sqlparser.ShowTables,
// 		*sqlparser.ShowVariables,
// 		*sqlparser.ShowColumns,
// 		*sqlparser.ShowIndex,
// 		*sqlparser.DescribeTable:
// 		err = session.handleStmtQuery(stmt, data[pos:])
// 	default:
// 		err = session.handleStmtExec(stmt, data[pos:])
// 	}

// 	return err
// }

// func (session *Session) handleStmtExec(stmt *mysql.Stmt, data []byte) error {

// 	var rs mysql.Result
// 	var err error

// 	if len(data) > 0 {
// 		rs, err = stmt.Exec(driver.RawStmtParams(data))
// 	} else {
// 		rs, err = stmt.Exec()
// 	}

// 	if err != nil {
// 		return session.handleMySQLError(err)
// 	}

// 	return session.fc.WriteOK(rs)
// }

// func (session *Session) handleStmtQuery(stmt *mysql.Stmt, data []byte) error {
// 	var rows mysql.Rows
// 	var err error

// 	if len(data) > 0 {
// 		rows, err = stmt.Query(driver.RawStmtParams(data))
// 	} else {
// 		rows, err = stmt.Query()
// 	}

// 	if err != nil {
// 		return session.handleMySQLError(err)
// 	}

// 	return session.writeRows(rows)
// }

// func (session *Session) handleComStmtSendLongData(data []byte) error {
// 	if len(data) < 6 {
// 		return session.handleMySQLError(mysql.ErrMalformPkt)
// 	}

// 	id := binary.LittleEndian.Uint32(data[0:4])

// 	stmt, ok := session.bc.stmts[id]
// 	if !ok {
// 		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
// 			strconv.FormatUint(uint64(id), 10), "stmt_send_longdata")
// 	}

// 	paramId := binary.LittleEndian.Uint16(data[4:6])
// 	if paramId >= uint16(len(stmt.Params)) {
// 		return mysql.NewDefaultError(mysql.ER_WRONG_ARGUMENTS, "stmt_send_longdata")
// 	}

// 	stmt.SendLongData(int(paramId), data[6:])
// 	return nil
// }

// func (session *Session) handleComStmtReset(data []byte) error {
// 	if len(data) < 4 {
// 		return session.handleMySQLError(mysql.ErrMalformPkt)
// 	}

// 	id := binary.LittleEndian.Uint32(data[0:4])

// 	stmt, ok := session.bc.stmts[id]
// 	if !ok {
// 		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
// 			strconv.FormatUint(uint64(id), 10), "stmt_reset")
// 	}

// 	if rs, err := stmt.Reset(); err != nil {
// 		return session.handleMySQLError(err)
// 	} else {
// 		return session.fc.WriteOK(rs)
// 	}
// }

// func (c *Session) handleComStmtClose(data []byte) error {
// 	if len(data) < 4 {
// 		return nil
// 	}

// 	id := binary.LittleEndian.Uint32(data[0:4])

// 	if cstmt, ok := c.bc.stmts[id]; ok {
// 		cstmt.Close()
// 	}

// 	delete(c.bc.stmts, id)

// 	return nil
// }
