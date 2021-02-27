import os
import pickle
import table
import database
import datetime

class Log:
    
    def save_old_table(db_object,table_name):
        #Save a copy of the table before changes so that rollback is possible
        file_path1 = 'old_tables.data'
        tables=[]
        # check if size of file is 0
        if os.stat(file_path1).st_size > 0:
            with open('old_tables.data', 'rb') as filehandle:
                # read the data as binary data stream
                tables= pickle.load(filehandle)
        tables.append(db_object.tables[table_name])
        with open('old_tables.data', 'wb') as filehandle:
            # store the data as binary data stream
            pickle.dump(tables, filehandle)
        #Save the insert_stack of the table
        file_path2 = 'old_stack.data'
        stack=[]
        if os.stat(file_path2).st_size > 0:
            with open('old_stack.data', 'rb') as filehandle:
                stack= pickle.load(filehandle)
        stack.append(db_object._get_insert_stack_for_table(table_name))
        with open('old_stack.data', 'wb') as filehandle:
            pickle.dump(stack, filehandle)
        #Add a record to log_data: <database name>, <table name>, <index name of the table if it exists>
        f = open('log_data.txt','a')
        f.write(str(db_object._name)+","+str(table_name) + "\n")
        f.close()


    def rollback(self,N,database_name=None,table_name=None):
        date = datetime.datetime.now()
        if table_name!=None and database_name!=None:
            self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK " +"DATABASE: "+str(database_name)+" TABLE: "+str(table_name))
        elif database_name!=None:
            self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK " +"DATABASE: "+str(database_name))
        else:
            self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK ")
        try: #if the fuction tries to rollback N steps while N > tables' queries, it will throw an exception terminating the rollback
            print('Initiating rollback.')
            #Save the contents of the log_data to a list
            lines = []
            # open file and read the content in a list
            with open('log_data.txt', 'r') as filehandle:
                filecontents = filehandle.readlines()
                for line in filecontents:
                    # here we remove the linebreak which is the last character of the string each time
                    curr_line = line[:-1]
                    # add item to the list
                    lines.append(curr_line)
            #Reverse the list so that the last actions in the database will be rollbacked first
            #Reverse gives us here the needed list since rollback follows the rule LIFO
            lines.reverse()
            if len(lines)<N:
                print("The rollback wasn't possible")
                self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK INTERRUPTED")
                return
            if database_name!=None and table_name!=None:
                for i in range(N):
                    if lines[i].count(database_name)>0 and lines[i].count(table_name)>0:
                        if lines[i].count('table_creation')>0 or lines[i].count('table_creation_from_csv')>0:
                            db = database.Database(database_name)
                            db.drop_table(table_name)
                        else:
                            file_path1 = 'old_tables.data'
                            file_path2 = 'old_stack.data'
                            tables=[]
                            stack=[]
                            if os.stat(file_path1).st_size > 0 and os.stat(file_path2).st_size > 0:
                                components = lines[i].split(",")
                                with open('old_tables.data', 'rb') as filehandle:
                                    tables= pickle.load(filehandle)
                                table_var=tables[len(tables)-1]
                                tables.pop(len(tables)-1)
                                with open('old_tables.data', 'wb') as filehandle:
                                    pickle.dump(tables, filehandle)
                                with open('old_stack.data', 'rb') as filehandle:
                                    stack= pickle.load(filehandle)
                                stack_var=stack[len(stack)-1]
                                stack.pop(len(stack)-1)
                                with open('old_stack.data', 'wb') as filehandle:
                                    pickle.dump(stack, filehandle)
                                db = database.Database(database_name)
                                db.drop_table(table_name,False)
                                db.table_from_object(table_var)
                                db._update_meta_insert_stack_for_tb(table_name,stack_var)
                                db._update()
                            else:
                                print('\nNO COMMANDS EXECUTED YET - CANNOT ROLLBACK\n')
                                print('Terminating rollback.')
                                self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK INTERRUPTED")
                                return
                print('Terminating rollback.')
                self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: END OF ROLLBACK")
                for i in range(N):
                    lines.pop(i)
                lines.reverse()
                with open('log_data.txt', 'w') as filehandle:
                    filehandle.writelines("%s\n" % place for place in lines)
            elif database_name!=None:
                for i in range(N):
                    if lines[i].count(database_name)>0:
                        if lines[i].count('table_creation')>0 or lines[i].count('table_creation_from_csv')>0:
                            db = database.Database(database_name)
                            db.drop_table(table_name)
                        elif lines[i].count('database_drop')>0:
                            print('\nCANNOT ROLLBACK DROPPED DATABASE\n')
                            print('Terminating rollback.')
                            self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK INTERRUPTED")
                            return
                        elif lines[i].count('database_creation')>0:
                            db = database.Database(database_name)
                            db.drop_db()
                        else:
                            file_path1 = 'old_tables.data'
                            file_path2 = 'old_stack.data'
                            tables=[]
                            stack=[]
                            if os.stat(file_path1).st_size > 0 and os.stat(file_path2).st_size > 0:
                                components = lines[i].split(",")
                                with open('old_tables.data', 'rb') as filehandle:
                                    # read the data as binary data stream
                                    tables= pickle.load(filehandle)
                                table_var=tables[len(tables)-1]
                                tables.pop(len(tables)-1)
                                with open('old_tables.data', 'wb') as filehandle:
                                    # store the data as binary data stream
                                    pickle.dump(tables, filehandle)
                                with open('old_stack.data', 'rb') as filehandle:
                                    # read the data as binary data stream
                                    stack= pickle.load(filehandle)
                                stack_var=stack[len(stack)-1]
                                stack.pop(len(stack)-1)
                                with open('old_stack.data', 'wb') as filehandle:
                                    # store the data as binary data stream
                                    pickle.dump(stack, filehandle)
                                db = database.Database(database_name)
                                db.drop_table(components[1],False)
                                db.table_from_object(table_var)
                                db._update_meta_insert_stack_for_tb(table_name,stack_var)
                                db._update()
                            else:
                                print('\nNO COMMANDS EXECUTED YET - CANNOT ROLLBACK')
                                self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK INTERRUPTED")
                                return
                print('Terminating rollback.')
                self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: END OF ROLLBACK")
                for i in range(N):
                    lines.pop(i)
                lines.reverse()
                with open('log_data.txt', 'w') as filehandle:
                    filehandle.writelines("%s\n" % place for place in lines)
            else:
                print("ROLLBACK CAN ONLY BE EXECUTED FOR A DATABASE OR A DATABASE'S TABLE")
                self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK INTERRUPTED")
        except:
            print("\nROLLBACK CANNOT CONTINUE!")
            self.analyt_log("TIMESTAMP: "+str(date)+" FUNCTION: ROLLBACK INTERRUPTED")

        
    def print_log(self):
        lines = []
        # open file and read the content in a list
        with open('user_log.txt', 'r') as filehandle:
            filecontents = filehandle.readlines()
            for line in filecontents:
                # here we remove the linebreak which is the last character of the string each time
                curr_line = line[:-1]
                # add item to the list
                lines.append(curr_line)
        for i in range(len(lines)):
            print(lines[i])
            
    @staticmethod
    def analyt_log(record):
    	#we append the analytical record to the user_log
        f = open("user_log.txt", "a")
        f.write(str(record))
        f.write("\n")
        f.close()

    
