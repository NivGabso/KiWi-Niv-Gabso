package kiwi;

import java.io.IOException;
import java.util.*;

import linearizability.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import util.Utils;

/**
 * Created by msulamy on 7/27/15.
 */
public class KiWiMap implements CompositionalMap<Integer,Integer>
{
	/***************	Constants			***************/
	
	/***************	Members				***************/
	public static boolean			SupportScan = true;
    public static int               RebalanceSize = 2;
    private LowerUpperBounds sizeBounds; // Bounds the map size from below and from above.

    public KiWi<Integer,Integer>	kiwi;
	private HistoryLogger historyLogger;

    /***************	Constructors		***************/
    public KiWiMap(){
        this(false, false);
    }

    public KiWiMap(boolean logOperations, boolean calculateSizeBounds)
    {
        sizeBounds = new LowerUpperBounds(!calculateSizeBounds);
    	ChunkInt.initPool(logOperations);
        KiWi.RebalanceSize = RebalanceSize;
    	this.kiwi = new KiWi<>(new ChunkInt(logOperations, sizeBounds), SupportScan, sizeBounds);
    	if(logOperations) {
            historyLogger = new HistoryLogger();
        }else{
    	    historyLogger = null;
        }
    }
    
    /***************	Methods				***************/

    /** same as put - always puts the new value! even if the key is not absent, it is updated */
    @Override
    public Integer putIfAbsent(Integer k, Integer v)
    {

        kiwi.put(k, v);
        return null;	// can implement return value but not necessary
    }
    
    /** requires full scan() for atomic size() */
    @Override
    public int size()
    {
        return -1;
    }
    
    /** not implemented ATM - can be implemented with chunk.findFirst() */
    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public Integer get(Object o)
    {
        if(historyLogger != null) {
            Get get = new Get((Integer) o, null);
            TimedOperation timedOperation = new TimedOperation(get);
            Integer res = kiwi.get((Integer) o);
            timedOperation.setEnd();
            get.setRetval(res);
            historyLogger.logOperation(timedOperation);
            return res;
        }else{
            return kiwi.get((Integer) o);
        }
    }

    @Override
    public Integer put(Integer k, Integer v)
    {
        if(historyLogger != null) {
            TimedOperation timedOperation = new TimedOperation(new Put(k, v));
            kiwi.put(k, v);
            timedOperation.setEnd();
            historyLogger.logOperation(timedOperation);
        }else{
            kiwi.put(k, v);
        }
        return null;
    }

    /** same as put(key,null) - which signifies to KiWi that the item is removed */
    @Override
    public Integer remove(Object o)
    {
        if(historyLogger != null) {
            TimedOperation timedOperation = new TimedOperation(new Discard((Integer)o));
            kiwi.put((Integer)o, null);
            timedOperation.setEnd();
            historyLogger.logOperation(timedOperation);
        }else{
            kiwi.put((Integer)o, null);
        }
        return null;
    }

    @Override
    public int getRange(Integer[] resultValues, Integer[] resultKeys, boolean addKeys,
                        Integer min, Integer max)
    {
        if(historyLogger != null){
            Scan scan = new Scan(min, max, null);
            TimedOperation timedOperation = new TimedOperation(scan);
            int res = kiwi.scan(resultValues, resultKeys, addKeys, min,max);
            timedOperation.setEnd();
            scan.setRetval(Utils.convertArrayToList(resultValues, res));
            historyLogger.logOperation(timedOperation);
            return res;
        }else{
            return kiwi.scan(resultValues, resultKeys, addKeys, min, max);
        }
    }
    
    /** same as put(key,val) for each item */
    @Override
    public void putAll(Map<? extends Integer, ? extends Integer> map)
    {
    	for (Integer key : map.keySet())
    	{
                kiwi.put(key, map.get(key));
        }
    }
    
    /** Same as get(key) != null **/
    @Override
    public boolean containsKey(Object o)
    {
    	return get(o) != null;
    }

    /** Clear is not really an option (can be implemented non-safe inside KiWi) - we just create new kiwi **/
    @Override
    public void clear()
    {
    	//this.kiwi.debugPrint();
    	ChunkInt.initPool(historyLogger != null);
    	sizeBounds = new LowerUpperBounds(sizeBounds.isFake);
    	this.kiwi = new KiWi<>(new ChunkInt(historyLogger != null, sizeBounds), SupportScan, sizeBounds);
    }

    /** Not implemented - can scan all & return keys **/
    @Override
    public Set<Integer> keySet()
    {
        throw new NotImplementedException();
    }

    /** Not implemented - can scan all & return values **/
    @Override
    public Collection<Integer> values()
    {
        throw new NotImplementedException();
    }

    /** Not implemented - can scan all & create entries **/
    @Override
    public Set<Entry<Integer,Integer>> entrySet()
    {
        throw new NotImplementedException();
    }    

    /** Not implemented - can scan all & search **/
    @Override
    public boolean containsValue(Object o)
    {
    	throw new NotImplementedException();
    }

    public void compactAllSerial()
    {
        kiwi.compactAllSerial();
    }
    public int debugCountDups()
    {
    	return kiwi.debugCountDups();
    }
    public int debugCountKeys()
    {
    	return kiwi.debugCountKeys();
    }
    public void debugPrint()
    {
    	kiwi.debugPrint();
    }

    public int debugCountDuplicates() { return kiwi.debugCountDuplicates();}
    public int debugCountChunks() {return 0; }

    public void calcChunkStatistics()
    {
        kiwi.calcChunkStatistics();
    }

    /** Upper bound size */
    public int sizeUpperBound()
    {
        if(historyLogger != null) {
            SizeUpperBound bound = new SizeUpperBound(null);
            TimedOperation timedOperation = new TimedOperation(bound);
            int res = kiwi.upperSizeBound();
            timedOperation.setEnd();
            bound.setRetval(res);
            historyLogger.logOperation(timedOperation);
            return res;
        }else{
            return kiwi.upperSizeBound();
        }
    }

    /** Upper bound size */
    public int sizeLowerBound()
    {
        if(historyLogger != null) {
            SizeLowerBound bound = new SizeLowerBound(null);
            TimedOperation timedOperation = new TimedOperation(bound);
            int res = kiwi.lowerSizeBound();
            timedOperation.setEnd();
            bound.setRetval(res);
            historyLogger.logOperation(timedOperation);
            return res;
        }else{
            return kiwi.lowerSizeBound();
        }
    }

    public void write(String dir) throws IOException{
        historyLogger.write(dir);
    }
}

