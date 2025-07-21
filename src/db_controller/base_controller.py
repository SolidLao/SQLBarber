from abc import ABC, abstractmethod
import os
import re
import datetime
import time

class BaseDBController(ABC):
    """ Base template to be extended to support various dbms (e.g., postgresql, mysql) """
    def __init__(self, db, user, password, restart_cmd, recover_script, port):
        
        self.db = db
        self.user = user
        self.password = password
        self.restart_cmd = restart_cmd
        self.config = {}
        self.knob_info = None
        self.connection = None
        self.timeout_s = 120
        self.failed_times = 0
        self.recover_script = recover_script
        self.port = port
        self._connect()

    @classmethod   
    def from_file(cls, config):
        db = config['DATABASE']['db']
        db_user = config['DATABASE']['user']
        password = config['DATABASE']['password']
        restart_cmd = config['DATABASE']['restart_cmd']
        recover_script = config['DATABASE']['recover_script']
        port = config['DATABASE']['port']
        
        return cls(db, db_user, password, restart_cmd, recover_script, port)
    
    def is_numerical(self, value):
        """ Returns true iff value is number, optionally followed by unit. """
        param_reg = r'[a-z_]+_[a-z]+'
        value_reg = r'(\d+(\.\d+)?)(%|\w*)'
        return True if re.match(value_reg + r'$', str(value)) else False

    def datetime_serializer(self, obj):
        """ Serialize datetime objects into string format """
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")
    
    def recover_dbms(self):
        """Recover the dbms if the dbms has a crash"""
        os.system(f"sh {self.recover_script}")
        print("DBMS recovered")

    def restart_dbms(self):
        os.system(self.restart_cmd)

    def safe_restart_dbms(self):
        """ 
            Restart to make parameter settings take effect. Returns true if successful.
            The configuration could make the dbms crash, so that maybe we need recovery operation.
        """
        self._disconnect()
        os.system(self.restart_cmd)
        time.sleep(2)
        success = self._connect()
        if success:
            return success
        else:
            try:
                self.recover_dbms()
                time.sleep(3)
                return True
            except Exception as e:
                print(f'Exception while trying to recover dbms: {e}')
                return False
    
    # move it in the future
    def create_template(self, test):
        self._copy_db(source_db="benchbase", target_db=f"{test}_template")
        print(f"created {test}_template for {test}")

        return True
    
    @abstractmethod
    def execute_queries_from_file(self, target_workload):
        """ execute queries in the ./customized_workloads/target_workload folder """
        pass

    @abstractmethod    
    def _connect(self):
        """ Establish connection to database, return success flag """
        pass
        
    @abstractmethod
    def _disconnect(self):
        """ Disconnect from database. """
        pass
    
    @abstractmethod
    def _copy_db(self, target_db, source_db):
        """ for tpcc, recover the data for the target db(benchbase) """
        pass

    @abstractmethod
    def update_dbms(self, sql):
        """ Execute sql query on dbms to update dbms and return success flag """
        pass

    @abstractmethod
    def execute_sql(self, sql):
        """ Execute sql on dbms and return the execution result """
        pass