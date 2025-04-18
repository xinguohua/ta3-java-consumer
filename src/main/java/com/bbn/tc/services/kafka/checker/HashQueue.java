package com.bbn.tc.services.kafka.checker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.log4j.Logger;

/**
 * HashSet that also maintains a queue that orders the elements in the order they were inserted
 * This is used to implement a max capacity.
 * When max capacity is reached, the oldest element is removed.
 * 
 * TODO: Use a ring buffer structure instead of generic array list so we're not allocating memory continually 
 * 
 * @author bbenyo
 *
 * @param <E>
 */
public class HashQueue<E> extends HashSet<E> {
  
    private static final long serialVersionUID = -28941957178941561L;

    private static final Logger logger = Logger.getLogger(HashQueue.class);

    protected int capacity = 100; 
    LinkedList<E> queue = new LinkedList<E>();
    
    public HashQueue(int capacity) {
        this.capacity = capacity;
    }
    
    @Override
    public boolean add(E element) {
        int sz = size();
        if (sz == capacity) {
            if (queue.size() > 0) {
                E delMe = queue.pop();
                this.remove(delMe);
            } else {
                logger.error("HashQueue backing queue is out of sync!");
            }
        } else if (sz > capacity) {
            logger.error("HashQueue size "+sz+" got over capacity: "+capacity);
            while (sz > capacity && queue.size() > 0) {
                E delMe = queue.pop();
                this.remove(delMe);
            }
        }
        
        boolean retval = super.add(element);
        if (retval) {
            queue.push(element);
        }
        return retval;
        
    }
}
