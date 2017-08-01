# gosql

A simple sql wrapper provides convenient CRUD operations for struct objects.
 
#### Mapping struct fields to sql columns
1. Column name is converted from field name with CamelToSnake pattern by default
1. Custom column name can be declared with db tag 
1. `primary key`, `auto_increment` are supported in db tag

        type Product struct {
    	    ID        int `db:"primary key,auto_increment"`
    	    Name      string
    	    Price     float32
    	    Text      string `db:"txt"`
    	    UpdatedAt int64
        }

#### Open database

    	db, err := Open("mysql", "dbuser:dbpassword@tcp(localhost:3306)/dbname")
    	...

#### Insert

        p := &Product{
            Name:      "apple",
            Price:     0.1,
            Text:      "nice",
            UpdatedAt: time.Now().Unix(),
        }
        db.Insert("products", p)
        
#### Update

        p.Price = 0.2
        db.Update("products", p)
        
#### Save
Save is supported by mysql and sqlite3 drivers. It will insert the record if it does't exist, otherwise update the record.
        p.Price = 0.3
        db.Save("products", p)
        
        p = &Product{
            Name:      "apple",
            Price:     0.1,
            Text:      "nice",
            UpdatedAt: time.Now().Unix(),
        }
        db.Save("products", p)
        
#### Select

        var products []*Product
        //Select all products
        db.Select("products", &products)
        
        //Select products whose price is less than 0.2
        db.Select("products", &products, "price<?", 0.2)
        
#### SelectOne

        var p1 *Product
        db.SelectOne("products", &p1)
     
        var p2 Product
        db.SelectOne("products", &p2, "id=?", 3)
        