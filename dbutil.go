package dbutil

import(
	"strconv"
	"reflect"
	"strings"
	"database/sql"
	_"github.com/go-sql-driver/mysql"
)

type DbUtil struct{
	database *sql.DB
	MaxIdle int
	Host string
	Port int
	Dbname string
}

func Create(host string, port int, dbname, user, password string)(*DbUtil,error){
	return CreateWithIdleSet(host,port,dbname,user,password,25)
}

func CreateWithIdleSet(host string,port int,dbname, user, password string,maxidle int)(*DbUtil,error){
	db,err:=sql.Open("mysql",user+":"+password+"@tcp("+host+":"+strconv.Itoa(port)+")/"+dbname+"?charset=utf8")
	if err!= nil{
		return nil,err
	}else{
		db.SetMaxIdleConns(maxidle)
		dbUtil := &DbUtil{
			database : db,
			MaxIdle : maxidle,
			Host : host,
			Port : port,
			Dbname : dbname,
		}
		return dbUtil,nil
	}
}

func (self *DbUtil)Query(sql string, params ...interface{})(*sql.Rows,error){
	if self.database != nil{
		stmt,err := self.database.Prepare(sql)
		if err != nil{
			return nil, err
		}else{
			defer stmt.Close()
			return stmt.Query(params...)
		}
	}
	return nil,&DbUtilError{"The database have no initial."}
}

func (self *DbUtil)QueryRow(sql string, params ...interface{})(*sql.Row, error){
	if self.database != nil{
		stmt,err := self.database.Prepare(sql)
		if err != nil{
			return nil, err
		}else{
			defer stmt.Close()
			return stmt.QueryRow(params...),nil
		}
	}
	return nil,&DbUtilError{"The database have no initial."}
}
/**
 * query one row as struct
 */
func (self *DbUtil)Query2Struct(struc interface{}, sql string, params ...interface{})(interface{},error){
	rowmap,err := self.Query2Map(sql,params...)
	if err != nil {
		return nil,err
	}else{
		return map2struct(struc,rowmap)
	}
}
/**
 * query a set of row as struct list
 */
func (self *DbUtil)Query2StructList(struc interface{}, sql string, params ...interface{})([]interface{},error){
	rowmapList,err := self.Query2MapList(sql,params...)
	if err != nil {
		return nil,err
	}else{
		leng := len(rowmapList)
		if leng>0 {
			var list []interface{}
			for _,rowmap := range rowmapList{
				val,err := map2struct(struc,rowmap)
				if err == nil{
					list = append(list, val)
				}
			}
			return list,nil
		}else{
			return nil, &DbUtilError{"The map list is nil."}
		}
	}
}
func map2struct(struc interface{},m map[string]interface{})(interface{},error){
	if struc==nil || m == nil{
		return nil,&DbUtilError{"The nil value."}
	}
	t := reflect.ValueOf(struc).Type()
	structmp := reflect.New(t).Interface()
	refValue := reflect.ValueOf(structmp).Elem()
	for key,value:=range m{
		field := refValue.FieldByNameFunc(func(f string)bool{
				if strings.ToLower(f) == key{
					return true
				}
				return false
			})
		if field.IsValid() && field.CanSet(){
			if value != nil{
				if field.Kind() == reflect.ValueOf(value).Kind(){
					field.Set(reflect.ValueOf(value))
				}else{
					switch field.Kind(){
						default:
							field.Set(reflect.ValueOf(value))
						case reflect.String:
							field.SetString(string(value.([]uint8)))
					}
				}
			}
		}
	}
	return structmp,nil
}
/**
 * query a set of rows as struct list
 */
/**
 * query one row as array
 */
func (self *DbUtil)Query2Array(sql string, params ...interface{})([]interface{},error){
	rows,err:=self.Query(sql,params...)
	if err != nil {
		return nil,err
	}else{
		defer rows.Close()
		cols,err:=rows.Columns()
		if err != nil{
			return nil, err
		}else{
			leng := len(cols)
			values := make([]interface{},leng)
			onerow := make([]interface{},leng)
			for i:=0; i<leng;i++{
				onerow[i] = &values[i]
			}
			
			if rows.Next(){
				rows.Scan(onerow...)
			}
			return values,nil
		}
	}
}
/**
 * query a set of row as array list.
 */
func (self *DbUtil)Query2ArrayList(sql string, params ...interface{})([][]interface{},error){
	rows,err := self.Query(sql, params...)
	if err != nil {
		return nil,err
	}else{
		defer rows.Close()
		cols,err:=rows.Columns()
		if err != nil {
			return nil, err
		}else{
			var list [][]interface{}
			leng := len(cols)
			for rows.Next(){
				values := make([]interface{},leng)
				onerow := make([]interface{},leng)
				for i:=0; i<leng;i++{
					onerow[i] = &values[i]
				}
				rows.Scan(onerow...)
				list = append(list, values)
			}
			return list,nil
		}
	}
}
/**
 * query one row to a map
 */
func (self *DbUtil)Query2Map(sql string, params ...interface{})(map[string]interface{},error){
	rows,err := self.Query(sql,params...)
	if err != nil {
		return nil, err
	}else{
		defer rows.Close()
		cols,err:=rows.Columns()
		if err != nil {
			return nil, err
		}else{
			rowmap := make(map[string]interface{})
			leng := len(cols)
			if rows.Next(){
				values := make([]interface{},leng)
				onerow := make([]interface{},leng)
				for i:=0; i<leng;i++{
					onerow[i] = &values[i]
				}
				rows.Scan(onerow...)
				for i:=0;i<leng;i++{
					rowmap[strings.ToLower(cols[i])] = values[i]
				}
			}
			return rowmap,nil
		}
	}
}
/**
 * query a set of row to a map list
 */
func (self *DbUtil)Query2MapList(sql string, params ...interface{})([]map[string]interface{},error){
	rows,err := self.Query(sql,params...)
	if err != nil {
		return nil, err
	}else{
		defer rows.Close()
		cols,err:=rows.Columns()
		if err != nil {
			return nil, err
		}else{
			var maplist []map[string]interface{}
			leng := len(cols)
			for rows.Next(){
				rowmap := make(map[string]interface{})
				values := make([]interface{},leng)
				onerow := make([]interface{},leng)
				for i:=0; i<leng;i++{
					onerow[i] = &values[i]
				}
				rows.Scan(onerow...)
				for i:=0;i<leng;i++{
					rowmap[strings.ToLower(cols[i])] = values[i]
				}
				maplist = append(maplist,rowmap)
			}
			return maplist,nil
		}
	}
}
/**
 * execute insert, update and delete
 */
func (self *DbUtil)Execute(sql string, params ...interface{})(int64,error){
	if self.database != nil{
		stmt,err := self.database.Prepare(sql)
		if err != nil{
			return 0,err
		}else{
			defer stmt.Close()
			res,err := stmt.Exec(params)
			if err!=nil {
				return 0,err
			}else{
				return res.RowsAffected()
			}
		}
	}
	return 0,&DbUtilError{"The database have no initial."}
}
/**
 * destroy the database pool.
 */
func (self *DbUtil)Destroy()error{
	if self.database!=nil {
		return self.database.Close()
	}
	return nil
}