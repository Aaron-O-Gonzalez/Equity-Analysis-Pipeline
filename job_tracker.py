import datetime 
from connect_database import connect_database, connect_to_server, create_table

class Tracker:
    '''Creates a tracker that assigns job ids and establishes a MySQL backend for successful 
    executions. Each log entry includes job_id, status, updated_time'''
    def __init__(self, jobname):
        self.jobname = jobname

    def assign_job_id(self):
        job_id = self.jobname + "_" + str(datetime.date.today())
        return job_id

    def connect_backend(self):
        server_connection = connect_to_server()
        db_connection = connect_database(server_connection)
        create_table(db_connection)
        return db_connection

    def update_job_status(self, status):
        self.job_id = self.assign_job_id()
        print(f"Job ID Assigned: {self.job_id}")
        update_time = datetime.datetime.now()
        connection = self.connect_backend()
        sql_statement = """
                            INSERT INTO jobs(job_id, status, updated_time)
                            VALUES(%s, %s, %s)
                        """
        values = (self.job_id, status, update_time,)
        
        try:
            cursor = connection.cursor()
            cursor.execute(sql_statement, values)
            connection.commit()
            cursor.close()
        except:
            print("Error updating job tracker")

    def get_job_status(self, job_id):
        connection = self.connect_backend()
        sql_statement = """ SELECT * FROM jobs WHERE job_id =%s"""
        values = (job_id,)
        
        try:
            cursor = connection.cursor()
            cursor.execute(sql_statement, values)
            row = cursor.fetchone()
            return (row[0] + ' ' + row[1] + ' ' + str(row[2]))

        except:
            print("Error retrieving information for job id. ")


def run_reporter_etl(success_status):
    tracker = Tracker('analytical_etl')
    if success_status:
        tracker.update_job_status("success")
        job_id = tracker.job_id
        job_status = tracker.get_job_status(job_id=job_id)
        print(job_status)

    else:
        tracker.update_job_status("failed")
        job_id = tracker.job_id
        job_status = tracker.get_job_status(job_id=job_id)
        print(job_status)

    return 