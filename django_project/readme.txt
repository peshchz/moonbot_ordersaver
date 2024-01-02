fix ‘str’ object has no attribute ‘decode’

1. find django 
python3 -c "import django; print(django.__path__)"

1. or see path on error message

2. cd db\backends\mysql\

3. sudo nano operations.py

4. replace 
# query = query.decode(errors='replace')
    query = errors='replace'
    

https://programmersought.com/article/76944839350/


bigint
ALTER TABLE test1 ADD COLUMN id bigserial PRIMARY KEY;