Here is how we can run the doctests from the dbsys-hw1 directory:

~/dbsys-hw1$ python3 -m Catalog.Schema -v

Trying:

    schema = DBSchema('employee', [('id', 'int'), ('dob', 'char(10)'), ('salary', 'int')])
    
Expecting nothing

ok

... [lots of testing output] ...

24 tests in 15 items.

24 passed and 0 failed.

Test passed.

In addition to the doctests, we have provided several unit tests that will be used to help grade your assignment. You should ensure that your code passes the tests on the ugrad cluster. To run the test suite, execute the following command from the top level dbsys-hw1directory.

$> python3 -m Tests.unit

You should see a report about which tests succeeded, followed by a more verbose explanation of the failures. You can inspect the test cases in Tests/unit.py.
