from Storage.Page import Page
from Storage.SlottedPage import SlottedPage
from Storage.File import StorageFile
from Storage.FileManager import FileManager
from Storage.BufferPool import BufferPool
from Catalog.Identifiers import FileId, PageId, TupleId
from Catalog.Schema import DBSchema

import sys
import unittest

# Change this to 'pageClass = SlottedPage' to test the SlottedPage class.
pageClass = SlottedPage

class Hw1PublicTests(unittest.TestCase):

  ###########################################################
  # File Class Tests
  ###########################################################
  # Utils:
  def makeSchema(self):
    return DBSchema('employee', [('id', 'int'), ('age', 'int')])

  def makeEmployee(self, n):
    schema = self.makeSchema()
    return schema.instantiate(n, 25 + n)

  def makeEmptyPage(self):
    schema = self.makeSchema()
    pId = PageId(FileId(1), 100)
    return pageClass(pageId=pId, buffer=bytes(4096), schema=schema)
  def makeDB(self):
    schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
    bp = BufferPool()
    fm = FileManager(bufferPool=bp)
    bp.setFileManager(fm)
    return (bp, fm, schema)

  def makePage(self, schema, fId, f,i):
    pId = PageId(fId, i)
    p = SlottedPage(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
    for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(1000)]:
      p.insertTuple(tup)
    return (pId, p)

  # Tests:
  def testFileAllocatePage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Keep allocating pages, making sure the number of pages in
    # the file is increasing.
    for i in range(10):
      f.allocatePage()
      self.assertEqual(f.numPages(), i+1)
    filem.close()

  def testFileAvailablePage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)

    # Since we aren't adding any data, 
    # The available page shouldn't change.
    # Even as we allocate more pages
    initialPage = f.availablePage().pageIndex
    for i in range(10):
      f.allocatePage()
      self.assertEqual(f.availablePage().pageIndex, initialPage)

    # Now we fill some pages to check that the available page has changed.
    for i in range(10):
      f.insertTuple(schema.pack(self.makeEmployee(i)))
    self.assertNotEqual(f.availablePage().pageIndex, initialPage)
    filem.close()

  def testFileInsertTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert 1000 tuples, checking the files numTuples()
    for i in range(10):
      f.insertTuple(schema.pack(self.makeEmployee(i)))
    self.assertEqual(f.numTuples(), 10)
    filem.close()

  def testFileDeleteTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    tids = []
    # Insert 1000 tuples, then delete them. File should have 0 tuples.
    for i in range(10):
      tids.append(f.insertTuple(schema.pack(self.makeEmployee(i))))
    for tid in tids:
      f.deleteTuple(tid)
    self.assertEqual(f.numTuples(), 0)
    filem.close()

  def testFileUpdateTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert and update a single tuple, then check the effect took hold.
    tid = f.insertTuple(schema.pack(self.makeEmployee(1)))
    f.updateTuple(tid, schema.pack(self.makeEmployee(10)))
    for tup in f.tuples():
      self.assertEqual(schema.unpack(tup).id, 10)
    filem.close()


if __name__ == '__main__':
  unittest.main(argv=[sys.argv[0], '-v'])
