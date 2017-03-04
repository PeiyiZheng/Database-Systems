import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

from Bitstring.bitstring import BitArray

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
# flag, setFlag, isDirty, setDirty
class SlottedPageHeader(PageHeader):
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  
  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True
  
  >>> ph.freeSpace() < ph.tupleSize
  True
  """

  def __init__(self, **kwargs):
    buffer     = kwargs.get("buffer", None)
    self.flags = kwargs.get("flags", b'\x00')
    if buffer:
      self.tupleSize       = kwargs.get("tupleSize", None)
      self.pageCapacity    = kwargs.get("pageCapacity", len(buffer))
      self.numSlots        = kwargs.get("numSlots", 0)
      self.nextSlot        = kwargs.get("nextSlot", 0)
      self.slotBuffer      = kwargs.get("slotBuffer", None)
      
      preBinrepr   = struct.Struct("cHHHH")
      preSize      = preBinrepr.size

      if self.slotBuffer == None:
        # 1 byte = 8 bits. Each tuple requires an extra bit in slot buffer.
        self.numSlots   = math.floor(8 * (self.pageCapacity - preSize) / (8 * self.tupleSize + 1))
        self.slotBuffer = BitArray('0b' + ('0' * self.numSlots))
      

      self.binrepr    = struct.Struct("cHHHH" + str(len(self.slotBuffer)) + 's')
      self.size       = self.binrepr.size

      buffer[0 : self.size] = self.pack()
    else:
      raise ValueError("No backing buffer supplied for SlottedPageHeader")

  def __eq__(self, other):
    return (    self.flags == other.flags
            and self.tupleSize == other.tupleSize
            and self.pageCapacity == other.pageCapacity
            and self.numSlots == other.numSlots
            and self.nextSlot == other.nextSlot
            and self.slotBuffer == other.slotBuffer)

  def __hash__(self):
    return hash((self.flags, self.tupleSize, self.pageCapacity, self.numSlots, self.slotBuffer))

  def numTuples(self):
    return self.slotBuffer.count('0b1')

  def headerSize(self):
    return self.size

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)
  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - (self.usedSpace() + self.headerSize())

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    return self.numTuples() * self.tupleSize

  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    return self.freeSpace() >= self.tupleSize

  # Slot operations.
  def offsetOfSlot(self, slot):
    # What is "slot" in the input parameters?
    if self.hasSlot(slot):
      return slot * self.tupleSize + self.headerSize()

  def hasSlot(self, slotIndex):
    return 0 <= slotIndex < self.numSlots

  # Check if the slot is used 
  def getSlot(self, slotIndex):
    return self.slotBuffer[slotIndex] 

  def setSlot(self, slotIndex, slot):
    if self.hasSlot(slotIndex):
      self.slotBuffer[slotIndex] = slot

  # Reset the slot to unused
  def resetSlot(self, slotIndex):
    self.setSlot(slotIndex, False)

  # Return a list of free slots
  def freeSlots(self):
    return [i for i in xrange(self.numSlots) if not self.getSlot(i)]

  # Return a list of used slots
  def usedSlots(self):
    return [i for i in xrange(self.numSlots) if self.getSlot(i)]

  # Tuple allocation operations.

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    if self.hasFreeTuple():
      nextFreeTuple = self.slotBuffer.find('0b0')

      # The reason for returning the bit position in a tuple is so that it evaluates as True
      # https://pythonhosted.org/bitstring/constbitarray.html#bitstring.Bits
      self.setSlot(nextFreeTuple[0], '0b1')

      return nextFreeTuple[0]
    else:
      return None

  def nextTupleRange(self):
    tupleIndex = self.nextFreeTuple()

    if tupleIndex != None:
      self.nextSlot = tupleIndex
      start = self.offsetOfSlot(self.nextSlot)
      return (self.nextSlot, start, start + self.tupleSize)
    else:
      return None
  
  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    return self.binrepr.pack(self.flags, self.tupleSize, self.pageCapacity, 
        self.numSlots, self.nextSlot, bytearray(self.slotBuffer))

  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def unpack(cls, buffer):
    tempBinrepr = struct.Struct("cHH")
    hBinrepr    = struct.Struct("H")
    numSlots    = hBinrepr.unpack_from(buffer, offset = tempBinrepr.size)[0]

    tempBuffer  = BitArray('0b' + ('0' * numSlots))
    binperp     = struct.Struct("cHHHH" + str(len(tempBuffer)) + 's')

    values = binperp.unpack_from(buffer)
    idx    = 0
    for bit in values[5]:
      if bit:
        tempBuffer[idx] = '0b1'
      else:
        tempBuffer[idx] = '0b0'

      idx += 1

    return cls(buffer = buffer, flags = values[0], tupleSize = values[1],
               pageCapacity = values[2], numSlots = values[3], 
               nextSlot = values[4], slotBuffer = tempBuffer)
    



######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage(Page):
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()  
  >>> p.clearTuple(tId)
  
  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
  
  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.
  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)

      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")
      
      #raise NotImplementedError

    else:
      raise ValueError("No backing buffer provided to page constructor.")

  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    # Find the next used page
    nextIdx = self.header.slotBuffer.find('0b1')

    if nextIdx == ():
      self.iterTupleIdx = -1
    else:
      self.iterTupleIdx = nextIdx[0]

    return self

  def __next__(self):
    t = self.getTuple(TupleId(self.pageId, self.iterTupleIdx))
    if t:
      nextIdx = self.header.slotBuffer.find('0b1', self.iterTupleIdx + 1)
      if nextIdx == ():
        self.iterTupleIdx = -1
      else:
        self.iterTupleIdx = nextIdx[0]

      return t
    else:
      raise StopIteration

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    if tupleId.tupleIndex < 0 or tupleId.tupleIndex >= self.header.numSlots or not self.header.slotBuffer[tupleId.tupleIndex]:
      return None
    else:
      buffer = self.getbuffer()
      offset = self.header.headerSize() + tupleId.tupleIndex * self.header.tupleSize

      return buffer[offset : offset + self.header.tupleSize]

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    if 0 <= tupleId.tupleIndex:
      buffer = self.getbuffer()
      offset = self.header.headerSize() + tupleId.tupleIndex * self.header.tupleSize
      buffer[offset : offset + self.header.tupleSize] = tupleData

      self.header.setSlot(tupleId.tupleIndex, '0b1')
      self.setDirty(True)

  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    tupleRange = self.header.nextTupleRange()

    if tupleRange:
      buffer = self.getbuffer()
      buffer[tupleRange[1] : tupleRange[2]] = tupleData
      
      self.header.setSlot(tupleRange[0], '0b1')
      self.setDirty(True)

      return TupleId(self.pageId, tupleRange[0])

  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    self.putTuple(tupleId, bytes(self.header.tupleSize))

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    self.header.setSlot(tupleId.tupleIndex, '0b0')
    self.setDirty(True)

  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    buffer = self.getbuffer()
    buffer[0 : self.header.headerSize()] = self.header.pack()
    return buffer

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    header = cls.headerClass.unpack(buffer)
    return cls(pageId = pageId , header = header, buffer = buffer)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
