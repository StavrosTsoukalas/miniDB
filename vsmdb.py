from database import Database
from log import Log

# create db with name "university"
db = Database('university', load=False)
# create a single table named "classroom"
db.create_table('classroom', ['building', 'room_number', 'capacity'], [str,str,int],'capacity')
# create a Log object which will be needed later
log1=Log()
# create B-tree index 'index1'
db.create_index('classroom','index1')
# insert 6 rows
db.insert('classroom', ['Packard', '101', '500'])
db.insert('classroom', ['Painter', '514', '10'])
db.insert('classroom', ['Taylor', '3128', '70'])
db.insert('classroom', ['Watson', '100', '30'])
db.insert('classroom', ['Watson', '120', '50'])
db.insert('classroom', ['Benjamin', '333', '180'])
db.delete('classroom','room_number==100')
db.update('classroom','222','room_number','room_number==333')
db.cast_column('classroom','capacity',str)
db.meta_insert_stack.show()
# calling rollback on database 'university', N==3
log1.rollback(3,'university')
# printing the classroom table and meta tables
db.show_table('classroom')
db.meta_insert_stack.show()
db.meta_length.show()
# printing the user_log.txt contents
log1.print_log()
