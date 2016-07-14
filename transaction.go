package dbutil

import(
	"database/sql"
	_"github.com/go-sql-driver/mysql"
)

type Tran struct{
	Tx *sql.Tx
}
/**
 * execute insert, update and delete
 */
func (self *Tran)Execute(sql string, params ...interface{})(int64,error){
	if self.Tx != nil{
		res,err := self.Tx.Exec(sql,params...)
		if err!=nil {
			return 0,err
		}else{
			return res.RowsAffected()
		}
	}
	return 0,&DbUtilError{"The database have no initial."}
}

func (self *Tran)Query(sql string, params ...interface{})(*sql.Rows,error){
	if self.Tx != nil{
		return self.Tx.Query(sql,params...)
	}
	return nil,&DbUtilError{"The database have no initial."}
}

func (self *Tran)QueryRow(sql string, params ...interface{})(*sql.Row,error){
	if self.Tx != nil{
		return self.Tx.QueryRow(sql,params...),nil
	}
	return nil,&DbUtilError{"The database have no initial."}
}

/**
 * commit
 */
func (self *Tran) Commit()error{
	if self.Tx != nil {
		return self.Tx.Commit()
	}
	return nil
}
/**
 * rollback
 */
func (self *Tran) Rollback()error{
	if self.Tx != nil {
		return self.Tx.Rollback()
	}
	return nil
}