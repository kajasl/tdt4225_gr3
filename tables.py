from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
import os
import time
<<<<<<< HEAD
from haversine import haversine, Unit
=======
from haversine import haversine
>>>>>>> 94e158101f53892551a5439c18c469867033c399


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
                    id INT NOT NULL PRIMARY KEY,
                    has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()


    def create_table_activity (self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id INT REFERENCES User(id),  
                    transportation_mode VARCHAR(50),
                    start_date_time DATETIME,
                    end_date_time DATETIME,

                    CONSTRAINT activity_fk 
                    FOREIGN KEY (user_id) REFERENCES User(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE
                    )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()


    def create_table_trackpoint (self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT REFERENCES Activity(id), 
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date_days DOUBLE,
                    date_time DATETIME,

                    CONSTRAINT trackpoint_fk 
                    FOREIGN KEY (activity_id) REFERENCES Activity(id) 
                    ON UPDATE CASCADE ON DELETE CASCADE
                    )
                """
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
        transportation = 'NULL'
        if label == [(True,)]:
            mode = open ('dataset/dataset/Data/' + user_id +'/labels.txt', 'r')
            transportation_mode = mode.read().splitlines()
            #pop redundant info first line
            transportation_mode.pop(0)

            transportation = {}

            for activity in transportation_mode:
                transp_mode = activity.split('\t')
                label_start_end_date = transp_mode[0].replace('/','').replace(':','').replace(' ','') + transp_mode[1].replace('/','').replace(':','').replace(' ','')
                transportation.update( {label_start_end_date : transp_mode[2] } )


        return transportation
                

    def insert_activities_and_trackpoints (self, user_id, has_labels, transportation, activity_id_start):

        path = 'dataset/dataset/Data/'+ user_id + '/Trajectory'

        #to keep incrementing activity id  for trackpoints
        activity_id = activity_id_start

        for (root, dirs, files) in os.walk(path):
            for fil in files:

                trackpoints_list = open(path + '/' + fil).read().splitlines()
                if len(trackpoints_list) <=2506:

                    start_date = (trackpoints_list[6].split(',')[5] +" "+ trackpoints_list[6].split(',')[6]).replace('-','').replace(':','').replace(' ','')
                    end_date = (trackpoints_list[-1].split(',')[5] +" "+ trackpoints_list[-1].split(',')[6]).replace('-','').replace(':','').replace(' ','')
                    
                    #couldve used NULL for database functionality
                    mode = 'none'
                    #a bit heavy to go through entire labels.txt every time to check if transportation mode should be added to activity
                    if transportation != 'none':

                        # start_date = datetime.strptime(activity_list[0], "%Y/%m/%d %H:%M:%S")
                        # end_date = datetime.strptime(activity_list[1], "%Y/%m/%d %H:%M:%S")

                        start_end_date = start_date + end_date
                        #print(transportation['hello'])
                        #check if activity should be labeled a transportation mode. Works only for transportation modes that last the entire activity
                        if start_end_date in transportation:
                            mode = transportation[start_end_date]



                    query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, '%s', %s, %s)"
                    self.cursor.execute(query % (user_id, mode, start_date, end_date)) 

                    #should be trackpoints_list[6:] but only a few lines so execution is fast
                    trackpoints_stripped = trackpoints_list[6:]

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

                        batch_trackpoints.append(tuple([str(activity_id), latitude, longitude, altitude, days, date]))
                        #print(tuple([str(activity_id), latitude, longitude, altitude, days, date]))
                        # query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
                        # self.cursor.execute(query % (activity_id, latitude, longitude, altitude, days, date)) 
                        
                        
                        

                    query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s) "
                    self.cursor.executemany(query, batch_trackpoints) 

                    activity_id += 1      
        self.db_connection.commit()
        return activity_id


            
            
            
    #kjør spørringer på denne, bare endre query til hva du vil
    def fetch_data(self, table_name, query):
        #query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        #print("Data from table %s, raw format:" % table_name)
        #print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    #kjør spørringer på denne, legg til x antall tables som siste argument for x tabeller
    def fetch_data_multiple_tables(self, query, *table_names):
        #query = "SELECT * FROM %s"
        self.cursor.execute(query % table_names)
        rows = self.cursor.fetchall()
        #print("Data from table %s, raw format:" % table_name)
        #print(rows)
        # Using tabulate to show the table in a nice way
        number_of_tables = "%s," * len(table_names)
        print(number_of_tables)
        string = "Data from table " + number_of_tables + " tabulated:"
        print(string % table_names)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def fetch_data_only_query(self, query):
        #query = "SELECT * FROM %s"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        #print("Data from table %s, raw format:" % table_name)
        #print(rows)
        # Using tabulate to show the table in a nice way
        string = "Data from table tabulated:"
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def fetch_data_without_print(self, query):
        #query = "SELECT * FROM %s"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        #print("Data from table %s, raw format:" % table_name)
        #print(rows)
        # Using tabulate to show the table in a nice way
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def calc_altitude(self):
        self.cursor
        rows = self.cursor.execute("SET @alt=0,@latest=0,@activity_id=''; select B.* from (select Trackpoint.altitude,A.change,IF(@activity_id<>A.activity_id,1,0) as LATEST,@activity_id:=A.activity_id as activity_id from (select activity_id,altitude-@alt as change, @alt:=altitude curr_altitude from TrackPoint WHERE activity_id = '300' order by activity_id) A order by activity_id) B where B.LATEST=1;")
        #rows = self.cursor.fetchall()
        #self.cursor.execute("")
        #query = "SELECT activity_id, @alt, @latest=altitude FROM TrackPoint WHERE activity_id='20';"
        #rows = self.cursor.execute(query)
        print(tabulate(rows, headers=self.cursor.column_names))
    #task 2.7
    def calculate_distance_haversine(self):
        query = """SELECT lat, lon 
            FROM TrackPoint join Activity 
            ON Activity.id = TrackPoint.activity_id  
            WHERE Activity.user_id='112' 
            AND YEAR(date_time)='2008' 
            AND Activity.transportation_mode='walk';"""
        self.cursor.execute(query)

        trackpoints = self.cursor.fetchall()
        sum_distance = 0
        for i in range(1, len(trackpoints)):
            sum_distance += haversine(trackpoints[i-1], trackpoints[i])
        #assume metric values
        print("User 112 in the year 2008 has walked: " + str("%.1f" % sum_distance) + "KM")




def main():

    program = None        

    try:
        program = Program()

        start = time.time()

        # screenshot of 11 first elements of each table to get a transportation mode
        # program.fetch_data("Activity", "SELECT * FROM %s LIMIT 11")
        # program.fetch_data("TrackPoint", "SELECT * FROM %s LIMIT 11")
        # program.fetch_data("User", "SELECT * FROM %s LIMIT 11")


        #task 2.1
        # program.fetch_data("Activity", "SELECT count(*) FROM %s")
        # program.fetch_data("TrackPoint", "SELECT count(*) FROM %s")
        # program.fetch_data("User", "SELECT count(*) FROM %s")

        #task 2.2
        #couldve used Avg but this was simplier
        #program.fetch_data_multiple_tables("SELECT (SELECT count(*) FROM %s) / (SELECT count(User.id)  FROM %s)", "Activity", "User")

        #task 2.3
        # program.fetch_data_only_query(
        #     """SELECT Activity.user_id, count(Activity.id) 
        #     FROM Activity JOIN User 
        #     WHERE User.id = Activity.user_id 
        #     GROUP BY User.id 
        #     ORDER BY count(Activity.id) 
        #     DESC LIMIT 20"""
        # )

        #task 2.4
        # program.fetch_data_only_query(
        #     """SELECT DISTINCT User.id, Activity.transportation_mode 
        #     FROM Activity JOIN User 
        #     WHERE User.id = Activity.user_id 
        #     AND Activity.transportation_mode = 'taxi' 
        #     GROUP BY User.id"""
        # )

        #task 2.5
        #program.fetch_data_only_query("SELECT Activity.transportation_mode, count(Activity.id) FROM Activity GROUP BY Activity.transportation_mode HAVING Activity.transportation_mode != 'none' ")

        #task 2.6
        #program.fetch_data_only_query("SELECT YEAR(Activity.start_date_time), count(Activity.id) FROM Activity GROUP BY YEAR(Activity.start_date_time)")

        # Task 2.8
        # query = """
        #     SELECT total_altitude.user_id AS "id", total_altitude.altitude_m AS "Total altitude in meters"
        #     FROM ( 
        #         SELECT 
        #             Activity.user_id AS user_id, 
        #             SUM(CASE WHEN tp1.altitude IS NOT NULL AND
        #             tp2.altitude IS NOT NULL 
        #             THEN (tp2.altitude - tp1.altitude) * 0.3048000 ELSE 0 END) AS altitude_m 
        #         FROM 
        #             TrackPoint AS tp1 JOIN TrackPoint AS tp2 ON tp1.activity_id=tp2.activity_id AND 
        #             tp1.id+1 = tp2.id JOIN Activity ON Activity.id = tp1.activity_id AND Activity.id = tp2.activity_id 
        #         WHERE tp2.altitude > tp1.altitude 
        #         GROUP BY Activity.user_id ) AS total_altitude 
        #     ORDER BY altitude_m DESC 
        #     LIMIT 20;
        # """
        # program.fetch_data_only_query(query)


        # program.fetch_data_only_query(
        #     """SELECT Activity.transportation_mode, count(Activity.id) 
        #     FROM Activity 
        #     GROUP BY Activity.transportation_mode 
        #     HAVING Activity.transportation_mode != 'none' """
        # )

        #task 2.6a
        # program.fetch_data_only_query(
        #     """SELECT YEAR(Activity.start_date_time), count(Activity.id) 
        #     FROM Activity 
        #     GROUP BY YEAR(Activity.start_date_time)"""
        # )

        #task 2.6b
        # program.fetch_data_only_query(
        #     """SELECT YEAR(Activity.start_date_time) as year, 
        #     count(Activity.id), 
        #     SUM(TIMESTAMPDIFF(hour, Activity.start_date_time, Activity.end_date_time)) as hours,
        #     SUM(TIMESTAMPDIFF(minute, Activity.start_date_time, Activity.end_date_time)) as minutes 
        #     FROM Activity GROUP BY YEAR(Activity.start_date_time)"""
        # )
        
        #task 2.7
        # program.calculate_distance_haversine()

        #fungerer ikke lag() er ikke innebygd
        #task 2.8
        # program.fetch_data_only_query(
        #     """SELECT User.id, SUM(select TrackPoint.*, greatest(altitude - lag(TrackPoint.altitude) over (order by TrackPoint.id), 0)) from TrackPoint)
        #     FROM TrackPoint JOIN Activity JOIN TrackPoint
        #     WHERE TrackPoint.activity_id = Activity.id
        #     AND Activity.user_id = User.id
        #     AND TrackPoint.altitude != -777
        #     GROUP BY User.id
        #     LIMIT 20"""
        # )
        
        #task 2.9
        # program.fetch_data_only_query(
        #     """SELECT Activity.user_id, COUNT(Activity.id)   
        #     FROM Activity 
        #     WHERE Activity.id IN (
        #         SELECT t1.activity_id
        #         From TrackPoint AS t1 INNER JOIN TrackPoint AS t2 ON t1.activity_id = t2.activity_id AND t1.id+1 = t2.id
        #         WHERE (t2.date_time < DATE_ADD(t1.date_time, INTERVAL 5 MINUTE)))
        #     GROUP BY Activity.user_id
        #     """)  
        
        # task 2.10 #Decimal rounds up latitude should be truncated instead, but no time to fix
        # program.fetch_data_only_query(
        #     """SELECT  User.id, Activity.id, TrackPoint.lat, TrackPoint.lon 
        #     FROM TrackPoint 
        #     INNER JOIN Activity ON TrackPoint.activity_id = Activity.id
        #     INNER JOIN User ON User.id = Activity.user_id
        #     WHERE cast(TrackPoint.lat as decimal(5,3)) = 39.916 
        #     AND cast(TrackPoint.lon as decimal(6,3))  =  116.397
        #     """
        # )

        # program.drop_table(table_name="TrackPoint")
        # program.drop_table(table_name="Activity")
        # program.drop_table(table_name="User")
        


        # program.create_table_user(table_name="User")
        # program.create_table_activity(table_name="Activity")
        # program.create_table_trackpoint(table_name="TrackPoint")
        # program.insert_users(table_name="User")
        #_ = program.fetch_data(table_name="User") 
        
        #send activity id between each insert_activities_and_trackpoints to keep incrementing
        # activity_id = 1
        # for id in program.ids:
        #     print("id changed ------------------------->", id)
        #     transp = program.transportation(user_id = id)
        #     activity_id = program.insert_activities_and_trackpoints(user_id = id, has_labels = program.labeled_ids, transportation = transp, activity_id_start = activity_id)



        program.show_tables()
        #takes around 30 min to insert, should be optimalized
        end = time.time()
        hours, rem = divmod(end-start, 3600)
        minutes, seconds = divmod(rem, 60)
        print("time taken -----> HH:MM:SSss {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

    

if __name__ == '__main__':
    main()
