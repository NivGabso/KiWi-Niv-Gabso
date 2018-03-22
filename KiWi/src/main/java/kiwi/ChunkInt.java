package kiwi;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkInt extends Chunk<Integer,Integer>
{
	private static AtomicInteger nextChunk;
	private static ChunkInt[] chunks;

	public static void setPoolSize(int numChunks) {
		chunks = new ChunkInt[numChunks];
	}

	public static void initPool(boolean delayForLinearizabilityTesting)
	{
		if (chunks != null)
		{
			nextChunk = new AtomicInteger(0);
			for (int i = 0; i < chunks.length; ++i)
				chunks[i] = new ChunkInt(null, null, delayForLinearizabilityTesting, new LowerUpperBounds(true));
			// use fake bounds calculator, newChunk() sets this element anyway.
		}
	}
	
	private static final int DATA_SIZE = 1;//Integer.SIZE/8;	// average # of BYTES of item in data array (guesstimate)
	public ChunkInt(){
		this(false, new LowerUpperBounds(true));
	}
	public ChunkInt(boolean delayForLinearizabilityTesting, LowerUpperBounds sizeBounds)
	{
		this(Integer.MIN_VALUE, null, delayForLinearizabilityTesting, sizeBounds);
	}

	public ChunkInt(Integer minKey, ChunkInt creator, boolean delayForLinearizabilityTesting){
		this(minKey, creator, delayForLinearizabilityTesting, new LowerUpperBounds(true));
	}

	public ChunkInt(Integer minKey, ChunkInt creator, boolean delayForLinearizabilityTesting, LowerUpperBounds sizeBounds)
	{
		super(minKey, DATA_SIZE, creator, delayForLinearizabilityTesting, sizeBounds);
	}

	@Override
	public Chunk<Integer,Integer> newChunk(Integer minKey)
	{
		if (chunks == null)
		{
			return new ChunkInt(minKey, this, delayForLinearizabilityTesting, sizeBounds);
		}
		else
		{
			int next = nextChunk.getAndIncrement();
			ChunkInt chunk = chunks[next];
			chunks[next] = null;
			chunk.minKey = minKey;
			chunk.creator = this;
			chunk.sizeBounds = sizeBounds;
			return chunk;
		}
	}
	
	@Override
	public Integer readKey(int orderIndex)
	{
		return get(orderIndex, OFFSET_KEY);
	}
	@Override
	public Object readData(int oi, int di)
	{
		/*
		// get data - convert next 4 bytes to Integer data
		int data = dataArray[di] << 24 | (dataArray[di+1] & 0xFF) << 16 |
			(dataArray[di+2] & 0xFF) << 8 | (dataArray[di+3] & 0xFF);
		*/

		return dataArray[di];
	}

    /**
     * Returns 1 if the version of orderIndex1 is newer.
     * 0 if equal, -1 if orderIndex2 is newer.
     * @param orderIndex1
     * @param orderIndex2
     * @return -1, 0, 1
     */
	int compareOIsVersion(int orderIndex1, int orderIndex2){
        if(orderIndex1 == orderIndex2){
            return 0;
        }
        if(orderIndex2 == NONE){
            return 1;
        }
        if(orderIndex1 == NONE){
            return -1;
        }
        if(getVersion(orderIndex1) > getVersion(orderIndex2)){
            return 1;
        }
        if(getVersion(orderIndex1) < getVersion(orderIndex2)){
            return -1;
        }
        if(Math.abs(get(orderIndex1, OFFSET_DATA)) > Math.abs(get(orderIndex2, OFFSET_DATA))){
            return 1;
        }
        return -1;
    }

    /**
     * Compare the tuple (is NONE, key)
     * 1 if the (is NONE, key) of orderIndex1 is larger.
     * @param orderIndex1
     * @param orderIndex2
     * @return -1 if orderIndex1 is smaller, 1 if orderIndex2 is smaller,
     *
     */
	int compareOIsKeys(int orderIndex1, int orderIndex2){
	    if(orderIndex1 == orderIndex2){
	        return 0;
        }
        if(orderIndex1 == NONE){
	        return 1;
        }
        if(orderIndex2 == NONE){
            return -1;
        }
        return Integer.compare(get(orderIndex1, OFFSET_KEY), get(orderIndex2, OFFSET_KEY));
    }

    @Override
    public void printData(int orderIndex) {
        System.out.format("tid=%d key=%d value=%d ver=%d oi=%d di=%d\n", KiWi.threadId(), get(orderIndex, OFFSET_KEY),
                    getData(orderIndex), getVersion(orderIndex), orderIndex, get(orderIndex, OFFSET_DATA));
    }

	@Override
	public int copyRange(Object[] resultValues, Object[] resultKeys, boolean addKeys,
						  int idx, int myVer, Integer min, Integer max, SortedMap<Integer, ThreadData.PutData<Integer,Integer>> items) {
	    // Fetch for each relevant key the corresponding value.
        // A key is relevant if it has the largest good version (<= myVer).
        // All items in the TreeMap are relevant.

        // We consider items in the linked list orderedArray and in the TreeMap.
        // Here, we iterate the TreeMap of extra items, and the linked list alternately.
		int i1LinkedListOI;
		if(idx == 0)
		{
			i1LinkedListOI = findFirst(min, myVer);
		} else{
			i1LinkedListOI = getFirst(myVer);
		}
		// Simplify the iteration over the TreeMap to iteration over a sorted array.
        List<Integer> treeMapKeys = new ArrayList<>(items.keySet());
		Collections.sort(treeMapKeys);
        int i2TreeMapIndex = 0;
        while(i2TreeMapIndex < treeMapKeys.size() && treeMapKeys.get(i2TreeMapIndex) < min){
            i2TreeMapIndex++;
        }
        int itemsCount = 0;
        int bestOI = -1;
        int lastKey = Integer.MIN_VALUE;
        // Like merge sort, in each iteration, advance either i1 or i2.
        while(i1LinkedListOI != NONE || i2TreeMapIndex < treeMapKeys.size()){
            int orderIndex1 = i1LinkedListOI;
            int orderIndex2 = NONE;
            if(i2TreeMapIndex < treeMapKeys.size()){
                orderIndex2 = items.get(treeMapKeys.get(i2TreeMapIndex)).orderIndex;
            }
            int compareKeys = compareOIsKeys(orderIndex1, orderIndex2);
            int currentOrderIndex;
            if(compareKeys >= 0) { // i2 is smaller, so it should be considered
                                   // (or i1==i2 - doesn't matter which is considered).
                currentOrderIndex = orderIndex2;
                i2TreeMapIndex++;
//                System.out.format("from TreeMap ");
            }else { // i1 is smaller, so it should be considered.
                currentOrderIndex = orderIndex1;
                i1LinkedListOI = get(i1LinkedListOI, OFFSET_NEXT);
//                System.out.format("from LinkedList ");
            }
//            printData(currentOrderIndex);
            int key = get(currentOrderIndex, OFFSET_KEY);
            if(key > max)
                break;
            if(key > lastKey){
                if(getVersion(currentOrderIndex) <= myVer) {
                    resultValues[idx + itemsCount] = getData(currentOrderIndex);
                    if(addKeys) {
						resultKeys[idx + itemsCount] = key;
					}
                    bestOI = currentOrderIndex;
                    lastKey = key;
                    itemsCount++;
                }
            }else{
                assert key == lastKey;
                assert bestOI != -1;
                if(getVersion(currentOrderIndex) <= myVer) {
                    if(compareOIsVersion(currentOrderIndex, bestOI) > 0){
//                        System.out.format("replacing oi=%d with oi=%d\n", bestOI, currentOrderIndex);
                        // Override previous selection
                        resultValues[idx + itemsCount - 1] = getData(currentOrderIndex);
                        bestOI = currentOrderIndex;
                    }else{
//                        System.out.println("not replacing");
                    }
                }
            }
        }
        return itemsCount;
	}

    @Override
    public void printLinkedList() {
        int current = 0;
        System.out.format("tid=%d\n", KiWi.threadId());
        do{
            if(current > 0)
                System.out.format("OI=%d, next=%d, ver=%d, key=%d, value=%d ->", current, get(current, OFFSET_NEXT),
                        get(current, OFFSET_VERSION), get(current, OFFSET_KEY), getData(current));
            current = get(current, OFFSET_NEXT);
        }while(current != NONE);
        System.out.println();
    }

    @Override
	public int allocate(Integer key, Integer data)
	{
		// allocate items in order and data array => data-array only contains int-sized data
		int oi = baseAllocate( data == null ? 0 : DATA_SIZE);
//        System.out.format("oi=%d, di=%d, val=%d\n", oi, get(oi, OFFSET_DATA), data);
		if (oi >= 0)
		{
			// write integer key into (int) order array at correct offset
			set(oi, OFFSET_KEY, (int) key);

			// get data index
			if(data != null) {
				int di = get(oi, OFFSET_DATA);
				assert di > 0;
				dataArray[di] = data;
			}

		}

		// return order-array index (can be used to get data-array index)
		return oi;
	}

	@Override
	public int allocateSerial(int key, Integer data)
	{
		// allocate items in order and data array => data-array only contains int-sized data
		int oi = baseAllocateSerial(data == null ? 0 : DATA_SIZE);

		if (oi >= 0)
		{
			// write integer key into (int) order array at correct offset
			set(oi, OFFSET_KEY, key);

			if(data != null) {
				int di = get(oi, OFFSET_DATA);
				dataArray[di] = data;
			}
		}

		// return order-array index (can be used to get data-array index)
		return oi;
	}

}
