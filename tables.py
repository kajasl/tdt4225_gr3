from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
import os


class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table_user(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id VARCHAR(3) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
#TODO add foreign key to both activity and trackpoint table
    def create_table_activity (self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id INT NOT NULL, 
                   transportation_mode VARCHAR(50),
                   start_date_time DATETIME,
                   end_date_time DATETIME)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_table_trackpoint (self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INT NOT NULL, 
                   lat DOUBLE,
                   lon DOUBLE,
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_users(self, table_name, ids, has_labels):
        for iden in ids:
            value=False
            for label_id in has_labels:
                if label_id == iden:
                    value=True
            query = "INSERT INTO %s (id, has_labels) VALUES (%s, %s)"
            self.cursor.execute(query % (table_name, iden, value))
        self.db_connection.commit()


    def transportation (self, user_id):
        query_label = "SELECT has_labels FROM User WHERE id = %s"
        self.cursor.execute(query_label % user_id)
        label = self.cursor.fetchall()
        transportation_mode = 0
        if label == [(1,)]:
            mode = open ('dataset/dataset/Data' + user_id + '/labels.txt', 'r')
            transportation_mode = mode.read().splitlines()
        return transportation_mode
                

    def insert_activity (self, user_id, has_labels, transportation):
        path = 'dataset/dataset/Data/'+ user_id + '/Trajectory'
        for (root, dirs, files) in os.walk(path):
            for fil in files:
                
                length = sum(1 for line in open(path + '/' + fil))
                if length <=2506:                    
                    filen = open(path + '/' + fil, "r")
                    trackpoints_list = filen.read().splitlines()
                    filen.close()
                    start_date = trackpoints_list[6].split(',')[5] + ' ' + trackpoints_list[6].split(',')[6]
                    end_date = trackpoints_list[-1].split(',')[5] + ' ' + trackpoints_list[-1].split(',')[6]
                    print (start_date)
                    print(end_date)
                    query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
                    #Denne funker ikke, men jeg har ikke  skjønt hva som er galt
                    self.cursor.execute(query % (user_id, 'transportation', start_date, end_date))
        self.db_connection.commit()
            
            
            

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    ids = []
    for (root,dirs,files) in os.walk('Dataset', topdown=True): 
        if (root == 'dataset/dataset/Data'):
            ids = (dirs)
            
    labels = open("dataset/dataset/labeled_ids.txt", "r")
    has_labels = labels.read().splitlines()
    labels.close()

    program = None        

    try:
        program = Program()
        transp = program.transportation(user_id='000')
        program.drop_table(table_name="User")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="TrackPoint")

        program.create_table_user(table_name="User")
        program.create_table_activity(table_name="Activity")
        program.create_table_trackpoint(table_name="TrackPoint")
        program.insert_users(table_name="User", ids = ids, has_labels=has_labels)
        
        program.insert_activity(user_id='000', has_labels=has_labels, transportation=transp)
        _ = program.fetch_data(table_name="Activity")        
        
        program.drop_table(table_name="User")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="TrackPoint")
        # Check that the table is dropped
        program.show_tables()


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
