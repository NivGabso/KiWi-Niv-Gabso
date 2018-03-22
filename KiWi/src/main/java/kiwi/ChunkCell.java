package kiwi;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.SortedMap;

public class ChunkCell extends Chunk<Cell, Cell>
{
	private static final int DATA_SIZE = 100;		// average # of BYTES of item in data array (guesstimate)
	
	public ChunkCell(boolean delayForLinearizabilityTesting)
	{
		this(Cell.Empty, null, delayForLinearizabilityTesting);
	}
	public ChunkCell(Cell minKey, ChunkCell creator, boolean delayForLinearizabilityTesting)
	{
		super(minKey, DATA_SIZE, creator, delayForLinearizabilityTesting);
	}
	@Override
	public Chunk<Cell,Cell> newChunk(Cell minKey)
	{
		return new ChunkCell(minKey.clone(), this, delayForLinearizabilityTesting);
	}
	
	
	@Override
	public Cell readKey(int orderIndex)
	{
		throw new NotImplementedException();
	}
	@Override
	public Object readData(int oi, int di)
	{

		throw new NotImplementedException();
	}

	@Override
	public int copyRange(Object[] resultValues, Object[] resultKeys, boolean addKeys, int idx, int myVer, Cell min, Cell max, SortedMap<Cell, ThreadData.PutData<Cell, Cell>> items) {
		throw new NotImplementedException();
	}

    @Override
    public void printLinkedList() {
        throw new NotImplementedException();
    }

    @Override
    public void printData(int orderIndex) {
        throw new NotImplementedException();
    }

    @Override
	public int allocate(Cell key, Cell data)
	{
		throw new NotImplementedException();
	}

	@Override
	public int allocateSerial(int key, Cell data) {
		throw new NotImplementedException();
	}
}
