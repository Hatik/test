using System;
using System.Data.SQLite;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;

namespace TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            //To store the index and column name from the file
            Dictionary<int, string> Columns = new Dictionary<int, string>();

            //To store datatypes
            Dictionary<string, string> ColumnDataTypes = new Dictionary<string, string>();
            ColumnDataTypes["id"] = "integer";
            ColumnDataTypes["dt"] = "datetime";
            ColumnDataTypes["product_id"] = "integer";
            ColumnDataTypes["amount"] = "double";

            // Define delimiter characters
            char[] delimiterChars = { '\t' };

            //Create Sample data
            string createQuery = @" create table if not exists products(id integer not null primary key, name text);
                                    insert into products (name) values ('A');
                                    insert into products (name) values ('B');
                                    insert into products (name) values ('C');
                                    insert into products (name) values ('D');
                                    insert into products (name) values ('E');
                                    insert into products (name) values ('F');
                                    insert into products (name) values ('G');
                                    create table if not exists orders(
                                                                        id integer not null CHECK(TYPEOF(id) = 'integer'), 
                                                                        dt datetime CHECK(JULIANDAY(dt) IS NOT NULL), 
                                                                        product_id integer CHECK(TYPEOF(product_id) = 'integer'),  
                                                                        amount real CHECK(amount > 0 AND TYPEOF(amount) = 'real'), 
                                                                        foreign key(product_id) references products(id)
                                                                     );"; 
            System.Data.SQLite.SQLiteConnection.CreateFile("myDB.db3");
            using (System.Data.SQLite.SQLiteConnection conn = new System.Data.SQLite.SQLiteConnection("data source=myDB.db3;foreign keys=true;"))
            {
                using (System.Data.SQLite.SQLiteCommand cmd = new System.Data.SQLite.SQLiteCommand(conn))
                {
                    conn.Open();
                    cmd.CommandText = createQuery;
                    cmd.ExecuteNonQuery();
                    // Read file from App_Data folder
                    string[] lines = System.IO.File.ReadAllLines(Directory.GetCurrentDirectory() + @"../../../App_Data/import.txt");

                    cmd.CommandText = "INSERT INTO orders (";
                    // Identify the column order from first row of the import file
                    string[] elements = lines[0].Split(delimiterChars);
                    for (int i = 0; i < elements.Length; i++)
                    {
                        Columns[i] = elements[i];
                        cmd.CommandText = cmd.CommandText + elements[i] + ", ";

                    }
                    cmd.CommandText = cmd.CommandText.Remove(cmd.CommandText.Length - 2);
                    cmd.CommandText = cmd.CommandText + ") VALUES (";
                    string temp = cmd.CommandText;
                    //Create Insert Statements
                    //As insert itself is slow, it is better to use transaction
                    using (SQLiteTransaction tr = conn.BeginTransaction())
                    {
                        int j = 0;
                        cmd.Transaction = tr;
                        for (int i = 1; i < lines.Length; i++)
                        {

                            try
                            {
                                cmd.CommandText = temp;
                                elements = lines[i].Split(delimiterChars);
                                for (j = 0; j < elements.Length; j++)
                                {
                                    switch (ColumnDataTypes[Columns[j]])
                                    {
                                        case "double":
                                            double columnDouble = Convert.ToDouble(elements[j]);
                                            cmd.CommandText = cmd.CommandText + "@VALUE" + j + ", ";
                                            cmd.Parameters.AddWithValue("@VALUE" + j, columnDouble);
                                            break;
                                        case "integer":
                                            int columnInt = Int32.Parse(elements[j]);
                                            cmd.CommandText = cmd.CommandText + "@VALUE" + j + ", ";
                                            cmd.Parameters.AddWithValue("@VALUE" + j, columnInt);
                                            break;
                                        default:
                                            cmd.CommandText = cmd.CommandText + "@VALUE" + j + ", ";
                                            cmd.Parameters.AddWithValue("@VALUE" + j, elements[j]);
                                            break;

                                    }

                                }
                                // Insert values, try catch exceptions

                                cmd.CommandText = cmd.CommandText.Remove(cmd.CommandText.Length - 2);
                                cmd.CommandText = cmd.CommandText + ")";
                                try
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("On line " + (i + 1) + " caught exception: " + ex.Message + "\nThe Insert Statement: " + cmd.CommandText + "\nValues: " + string.Join(", ", elements) + "\n");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("On line " + (i + 1) + " caught exception: " + ex.Message + "\nColumn: " + Columns[j] + ", value: " + elements[j]);
                                break;
                            }


                        }
                        tr.Commit();

                    }



                    cmd.CommandText = "Select * from orders";
                    cmd.ExecuteNonQuery();
                    using (System.Data.SQLite.SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("ID |    Datetime     |Product_ID|Amount|");
                        while (reader.Read())
                        {
                            Console.WriteLine(reader["ID"] + " | " + reader["dt"] + " | " + reader["product_id"] + " | " + reader["amount"]);
                        }

                    }
                    // SQL query 1
                    cmd.CommandText = @"select 
                                                a.product_id, 
                                                count(*) as cnt, 
                                                sum(amount) as sm
                                        from orders a 
                                        where strftime('%m', dt) = '10' and strftime('%Y', dt) = '2017'
                                        group by product_id;";
                    cmd.ExecuteNonQuery();
                    using (System.Data.SQLite.SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("\n1.Query");
                        Console.WriteLine(" product_id |  count  | Sum ");
                        while (reader.Read())
                        {
                            Console.WriteLine(reader["product_id"] + " | " + reader["cnt"] + " | " + " | " + reader["sm"]);
                        }

                    }
                    // SQL query 2.a
                    cmd.CommandText = @"select
                                                a.product_id
                                                from orders a 
                                        where strftime('%m', dt) = strftime('%m', date('now')) and strftime('%Y', dt) = strftime('%Y', date('now')) and
                                              product_id not in (select product_id from orders where strftime('%m', dt) = strftime('%m', date('now', '-1 month')) and strftime('%Y', dt) = strftime('%Y', date('now')) )
                                        group by product_id
                                        ;";
                    cmd.ExecuteNonQuery();
                    using (System.Data.SQLite.SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("2.a query\nproduct_id ");
                        while (reader.Read())
                        {
                            Console.WriteLine(reader["product_id"]);
                        }

                    }
                    // SQL query 2.b
                    cmd.CommandText = @"select
                                                a.product_id
                                                from orders a 
                                        where strftime('%m', dt) = strftime('%m', date('now', '-1 month')) and strftime('%Y', dt) = strftime('%Y', date('now')) and
                                              product_id not in (select product_id from orders where strftime('%m', dt) = strftime('%m', date('now')) and strftime('%Y', dt) = strftime('%Y', date('now')) )
                                        group by product_id
                                        ;";
                    cmd.ExecuteNonQuery();
                    using (System.Data.SQLite.SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("2.b query\nproduct_id ");
                        while (reader.Read())
                        {
                            Console.WriteLine(reader["product_id"]);
                        }

                    }

                    //SQL query 3
                   cmd.CommandText = @"select 
		                                        strftime('%m', a.dt) || '/' || strftime('%Y', a.dt) as Period, 
		                                        a.product_id, max(sm) as Mx, 
		                                        max(sm)*100 /b.total as Percentage
                                        from 
		                                        (select 
				                                        dt, 
				                                        product_id, 
				                                        sum(amount) as sm 
		                                        from orders 
		                                        group by 
				                                        strftime('%Y', dt), 
				                                        strftime('%m', dt), 
				                                        product_id) a 
		                                        left join 
		                                        (select 
				                                        strftime('%m', dt) as month,
				                                        strftime('%Y', dt) as year,
				                                        sum(amount) as total 
		                                        from orders 
		                                        group by 
				                                        strftime('%Y', dt), 
				                                        strftime('%m', dt)) b 
		                                        on strftime('%m',a.dt) = b.month and  strftime('%Y',a.dt) = b.year
                                        group by 
	                                        strftime('%m', a.dt), 
	                                        strftime('%Y', a.dt);";
                    cmd.ExecuteNonQuery();
                    using (System.Data.SQLite.SQLiteDataReader reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("\n3. ");
                        Console.WriteLine(" Period | Product_id | Sum | Percentage |");
                        while (reader.Read())
                        {
                            Console.WriteLine(reader["Period"] + " | " + reader["product_id"] + " | " + reader["Mx"] + " | " + reader["Percentage"]);
                        }
                    }

                    conn.Close();
                }
            }
            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
        }
    }
}
