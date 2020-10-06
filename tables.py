from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
import os


class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.labeled_ids = open("dataset/dataset/labeled_ids.txt", "r").read().splitlines()
        self.ids = os.listdir("dataset/dataset/Data")
        #remove DS_Store element which would be first item and redundant
        self.ids.pop(0)

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
                    end_date_time DATETIME
                    )
                """
                           
                # FOREIGN KEY (user_id) 
                #     REFERENCES User(id) 
                #     ON DELETE CASCADE
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
                    date_time DATETIME
                    )
                """
                #     FOREIGN KEY (activity_id) 
                #         REFERENCES Activity(id)
                #         ON DELETE CASCADE
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_users(self, table_name):
        for iden in self.ids:
            value = False
            for label_id in self.labeled_ids:
                if label_id == iden:
                    value = True
            query = "INSERT INTO %s (id, has_labels) VALUES (%s, %s)"
            self.cursor.execute(query % (table_name, iden, value))
        self.db_connection.commit()


    def transportation (self, user_id):
        query_label = "SELECT has_labels FROM User WHERE id = %s"
        self.cursor.execute(query_label % user_id)
        label = self.cursor.fetchall()
        transportation_mode = 'none'
        if label == [(True,)]:
            mode = open ('dataset/dataset/Data/' + user_id +'/labels.txt', 'r')
            transportation_mode = mode.read().splitlines()
            #pop redundant info first line
            transportation_mode.pop(0)
        return transportation_mode
                

    def insert_activities_and_trackpoints (self, user_id, has_labels, transportation, activity_id_start):

        path = 'dataset/dataset/Data/'+ user_id + '/Trajectory'
        print("start of func")

        #to keep incrementing activity id  for trackpoints
        activity_id = activity_id_start

        for (root, dirs, files) in os.walk(path):
            print("os walk")
            for fil in files:

                trackpoints_list = open(path + '/' + fil).read().splitlines()
                if len(trackpoints_list) <=2506:

                    start_date = (trackpoints_list[6].split(',')[5] +" "+ trackpoints_list[6].split(',')[6]).replace('-','').replace(':','').replace(' ','')
                    end_date = (trackpoints_list[-1].split(',')[5] +" "+ trackpoints_list[-1].split(',')[6]).replace('-','').replace(':','').replace(' ','')
    
                    mode = 'none'
                    #a bit heavy to go through entire labels.txt every time to check if transportation mode should be added to activity
                    if transportation != 'none':
                        for activity in transportation:
                            activity_list = activity.split('\t')

                            # start_date = datetime.strptime(activity_list[0], "%Y/%m/%d %H:%M:%S")
                            # end_date = datetime.strptime(activity_list[1], "%Y/%m/%d %H:%M:%S")

                            label_start_date = activity_list[0].replace('/','').replace(':','').replace(' ','')
                            label_end_date = activity_list[1].replace('/','').replace(':','').replace(' ','')
                            
                            #check if activity should be labeled a transportation mode. Works only for transportation modes that last the entire activity
                            if label_start_date == start_date and label_end_date == end_date:
                                mode = activity_list[2]



                    query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, '%s', %s, %s)"
                    self.cursor.execute(query % (user_id, mode, start_date, end_date)) 

                    #should be trackpoints_list[6:] but only a few lines so execution is fast
                    trackpoints_stripped = trackpoints_list[6:12]

                    #list to hold trackpoint information to be able to executemany
                    batch_trackpoints = []
                    for trackpoint in trackpoints_stripped:

                        trackpoint_line = trackpoint.split(',')

                        latitude = (trackpoint_line[0])
                        longitude = (trackpoint_line[1])
                        altitude = (trackpoint_line[3])
                        days = (trackpoint_line[4])
                        date = (trackpoint_line[5] + trackpoint_line[6]).replace('-','').replace(':','').replace(' ','')


                        #batch_trackpoints.append(tuple(trackpoint_line))
                        ##trying to make executemany work to send a lot of trackpoints at the same time, but there is some issue about the tuple format 
                        ##TODO lookup cursor.executemany 
                        batch_trackpoints.append(tuple([str(activity_id), latitude, longitude, altitude, days, date]))
                        print(tuple([str(activity_id), latitude, longitude, altitude, days, date]))
                        # query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
                        # self.cursor.execute(query % (activity_id, latitude, longitude, altitude, days, date)) 
                        
                        
                        

                    query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
                    self.cursor.executemany(query, batch_trackpoints) 

                    activity_id += 1      
        self.db_connection.commit()
        return activity_id


            
            
            

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

    program = None        

    try:
        program = Program()

        program.drop_table(table_name="User")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="TrackPoint")


        program.create_table_user(table_name="User")
        program.create_table_activity(table_name="Activity")
        program.create_table_trackpoint(table_name="TrackPoint")
        program.insert_users(table_name="User")
        #_ = program.fetch_data(table_name="User") 

        # for user in program.ids:
            # transp = program.transportation(user_id=user)
            # program.insert_activity(user_id=user, has_labels=program.labeled_ids, transportation=0)
        
        #send activity id between each insert_activities_and_trackpoints to keep incrementing
        transp = program.transportation(user_id = program.ids[0])
        activity_id_continue = program.insert_activities_and_trackpoints(user_id = program.ids[0], has_labels = program.labeled_ids, transportation = transp, activity_id_start = 1)
        transp = program.transportation(user_id = program.ids[10])
        program.insert_activities_and_trackpoints(user_id = program.ids[10], has_labels = program.labeled_ids, transportation = transp, activity_id_start = activity_id_continue)

        _ = program.fetch_data(table_name="Activity")
        #_ = program.fetch_data(table_name="TrackPoint")      
        
        # program.drop_table(table_name="User")
        # program.drop_table(table_name="Activity")
        # program.drop_table(table_name="TrackPoint")
        # Check that the table is dropped

        program.show_tables()


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
