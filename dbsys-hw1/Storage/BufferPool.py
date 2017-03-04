import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)

    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?
    self.freeList     = [i for i in range(self.numPages())] 
    self.pageMap      = OrderedDict()


  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return len(self.freeList)

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    return pageId in self.pageMap
  
  def getPage(self, pageId):
    if not self.hasPage(pageId):
      # If no free pages in the list, call evictPage() to get one
      if self.numFreePages() == 0:
        self.evictPage()

      offset = self.freeList.pop(0) * self.pageSize
      buffer = self.pool.getbuffer()
      pageBuffer = buffer[offset : offset + self.pageSize]
      page = self.fileMgr.readPage(pageId, pageBuffer)
      #buffer[offset : offset + self.pageSize] = page.pack()
      
      self.pageMap[pageId] = offset
      self.pageMap.move_to_end(pageId) # LRU

      #print(page.header.numTuples())

      return page
    else:
      offset = self.pageMap[pageId]
      buffer = self.pool.getbuffer()
      pageBuffer = buffer[offset : offset + self.pageSize]

      # Code from FileManager.py
      rFile = self.fileMgr.fileMap.get(pageId.fileId, None)
      if rFile:
        page = rFile.pageClass().unpack(pageId, pageBuffer)
      else:
        page = None

      self.pageMap.move_to_end(pageId) # LRU

      #print(page.header.numTuples())

      return page

  # Keep the consistency between buffer and memory 
  def updatePage(self, pageId, page):
    if not self.hasPage(pageId):
      # If no free pages in the list, call evictPage() to get one
      if self.numFreePages() == 0:
        self.evictPage()

      offset = self.freeList.pop(0) * self.pageSize
      self.pageMap[pageId] = offset

    offset = self.pageMap[pageId]
    buffer = self.pool.getbuffer()
    buffer[offset : offset + self.pageSize] = page

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    if not self.hasPage(pageId):
      return

    self.pageMap.pop(pageId)
    self.freeList.append(pageId.pageIndex)

  def flushPage(self, pageId):
    if not self.hasPage(pageId):
      return

    offset = self.pageMap[pageId]
    buffer = self.pool.getbuffer()[offset : offset + self.pageSize]
    page = self.fileMgr.readPage(pageId, buffer)

    if page.header.isDirty():
      self.fileMgr.writePage(page)

    self.discardPage(pageId)

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    pageId = next(iter(self.pageMap.items()))[0]
    self.flushPage(pageId)

  # Flushes all dirty pages
  def clear(self):
    for key, val in self.pageMap.items():
      buffer = self.pool.getbuffer()[val : val + self.pageSize]
      page = self.fileMgr.readPage(key, buffer)

      if page.header.isDirty():
        self.flushPage(key)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
