Approach Applied in this implementation

Mapper - Reads the file, creates a key-value pair using the keyjoin value and tuple.

Reducer - splits the data using table name, inserts these into tables using the joining key (output from mapper), if the tuples keys match then they are put together and added to the result (output).

Driver -  runs the whole program, initalizes above classes using input arguments i.e. input hadoop file & specified output file.
