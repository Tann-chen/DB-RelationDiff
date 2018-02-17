package test;

/** Data for QuickSortInsertion from TestSort
   Original is based on Baase, 
   opt is based on Knuth, ending in an insertion sort

Sort times in millisecs, (10 times averaged), list size 100000:
    Avg    Low   High  Version Description-----
     19     15     32 opt, insert 25 random, 316 values
    127     79    313 original random, 316 values
     31     31     32 opt, insert 25 random
     52     46     63 original random
      5      0     16 opt, insert 25 in order
  18769  18359  19578 original in order
     17     15     31 opt, insert 25 in order except for 5%
     94     78    109 original in order except for 5%
     14      0     31 opt, insert 25 reversed order
  40605  39797  42985 original reversed order
     20     15     31 opt, insert 25 reversed except for 5%
     80     62    109 original reversed except for 5%
     14      0     31 opt, insert 25 all same
  20630  18313  22063 original all same

Random list of size 1000000:
Enter next quicksort insertion parameter: 1
Average time: 450
Enter next quicksort insertion parameter: 25
Average time: 383
Enter next quicksort insertion parameter: 20
Average time: 394
Enter next quicksort insertion parameter: 30
Average time: 392
Enter next quicksort insertion parameter: 25
Average time: 383

*/
public class QuickSort {
  public static int MAX_INSERTION_SORT = 25;  // parameter to tune for speed
  
  /**
   * Sort with a variant of Hoare's quicksort algorithm.
   * @param data 
   *   array containing elements to sort
   * @param start 
   *   index of first element to be sorted
   * @param end 
   *   index of final element to be sorted
   */
  public static void quicksort(int[ ] data, int start, int end) {
    // nonrecursive, efficient inner loop,
    // limited stack size, OK if list already sorted
    
    int pivotIndex; // Array index for the pivot element
    int first, last;  // start, end of current part
    int n1, n2; // Number of elements before and after the pivot element
    int[] stack = new int[100]; //Stack to hold index ranges
    // allows 2^50 elements to be sorted!
    int top = -1;

    //Fill stack with initial values in preparation for loop
    if (end - start >= MAX_INSERTION_SORT) {
        stack[++top] = start;
        stack[++top] = end;  
    }
    while(top >= 0) {
      //Get segment length & first index values
      last = stack[top--]; 
      first = stack[top--];

      // Partition the array, and set the pivot index.
      pivotIndex = partition(data, first, last);
//      System.out.println("Pivot index: " + pivotIndex);
//      TestSort.printArray(data);

      // Compute the sizes of the two pieces.
      n1 = pivotIndex - first;
      n2 = last - pivotIndex;

      // Push the n & first values of the array segments before & after
      // the pivotIndex onto the stack.  Make sure the larger of the
      // two segments is pushed first.  Only push a segment if its
      // length is > 1
      if(n2 < n1) {
        if(n1 > MAX_INSERTION_SORT) {
           stack[++top] = first;
           stack[++top] = pivotIndex-1;
        }
        if(n2 > MAX_INSERTION_SORT) {
           stack[++top] = pivotIndex+1;
           stack[++top] = last;
        }
      }
      else {
        if(n2 > MAX_INSERTION_SORT){
          stack[++top] = pivotIndex+1;
          stack[++top] = last;
        }
        if(n1 > MAX_INSERTION_SORT){
           stack[++top] = first;
           stack[++top] = pivotIndex - 1;
         } 
      } // end push depending on size
    } // end while loop for stack
    if (MAX_INSERTION_SORT > 1)  // test allows a check of qsort part
        insertionsort(data, start, end); //use original ends of region
  } // end quicksort


  private static int partition(int[ ] data, int first, int last) {
    // This version relies on sentinels to make inner loops faster.
    // Precondition: last>first, and data from index first through last.
    // Postcondition: The method has selected some "pivot value" that occurs
    // in data[first]...data[last]. The elements of data have then been
    // rearranged and the method returns a pivot index so that
    // -- data[pivot index] is equal to the pivot;
    // -- each element before data[pivot index] is <= the pivot;
    // -- each element after data[pivot index] is >= the pivot.
    int iLo = first + 1, iHi = last; //lowest, highest untested indices
    int mid = (first + last)/2; // take pivot from here
    int pivot = data[mid];        //    so in-order not worst case
    
    // Almost the body of main while loop follows, except for non-sentinal search
    //Swap the chosen pivot value into beginning
    data[mid] = data[first];
    data[first] = pivot;  //serves as first sentinel for downward sweep

    while (data[iHi] > pivot) // normal downward scan
      iHi--;
    while (iLo <= iHi && data[iLo] < pivot) // no sentinel upward yet
        iLo++;
    if (iLo <= iHi) {
      int temp = data[iLo];
      data[iLo] = data[iHi];
      data[iHi] = temp;
      iHi--;
      iLo++;
    }
    
    while (iLo <= iHi) { // now have sentinels both ways
      while (data[iHi] > pivot) 
        iHi--;
      while (data[iLo] < pivot) // have sentenel swapped in place now
        iLo++;
      if (iLo <= iHi) {
        int temp = data[iLo];
        data[iLo] = data[iHi];
        data[iHi] = temp;
        iHi--;
        iLo++;
      }
    }
    int iPivot = iLo-1;          // place of last smaller value
    data[first] = data[iPivot];  // swap with pivot
    data[iPivot] = pivot;
    return iPivot;
  } 

  public static void insertionsort(int[ ] data, int start, int end) {
    for (int next = start + 1; next <= end; next++) {
      if (data[next-1] > data[next]) {
        int val = data[next];
        int gap = next-1;
        data[next] = data[gap];
        while (gap > start && data[gap-1] > val) {
          data[gap] = data[gap-1];
          gap--;
        }
        data[gap] = val;
      } // end: if not already in place
    } // end: for each element to be inserted
  }

  /**
   * Sort with a variant of Hoare's quicksort algorithm.  
   * Behavior can be bad on large lists due to O(n) stack.
   * @param data 
   *   array containing elements to sort
   * @param first 
   *   index of first element to be sorted
   * @param last 
   *   index of final element to be sorted
   */
  public static void quicksortBadStack(int[ ] data, int first, int last) {
    // similar to book version: recursive, no sentinels
    if (last > first) {
      int pivotIndex = partitionOrig(data, first, last);
      quicksortBadStack(data, first, pivotIndex - 1);          
      quicksortBadStack(data, pivotIndex + 1, last);          
    }
  } // end quicksort


  /**
   * Sort with a variant of Hoare's quicksort algorithm used in Baase.
   * @param data 
   *   array containing elements to sort
   * @param first 
   *   index of first element to be sorted
   * @param last 
   *   index of final element to be sorted
   */
  public static void quicksortOrig(int[ ] data, int first, int last) {
    // similar to book version: recursive, no sentinels
    while (last > first) {
      int pivotIndex = partitionOrig(data, first, last);
      if(pivotIndex - first < last - pivotIndex) {
        quicksortOrig(data, first, pivotIndex - 1);          
        first = pivotIndex + 1; // last unchanged          
      }
      else {
        quicksortOrig(data, pivotIndex + 1, last);          
        last = pivotIndex - 1; // first unchanged          
      } 
    }
  } // end quicksort

  private static int partitionOrig(int[ ] data, int first, int last) {
    // Precondition: n > 1, and data has at least n elements starting at
    // data[first].
    // Postcondition: The method has selected some "pivot value" that occurs
    // in data[first]...data[first+n-1]. The elements of data have then been
    // rearranged and the method returns a pivot index so that
    // -- data[pivot index] is equal to the pivot;
    // -- each element before data[pivot index] is <= the pivot;
    // -- each element after data[pivot index] is > the pivot.
    int pivot = data[first], // now index first is 'vacant' (overwritable)
        lowVac = first,      // 'vacant' index before leftmost untested index
        high = last;         // rightmost untested index
        
    while (lowVac < high) {
      // loop invarient:
      //   lowVac is vacancy just before the lowest untest index
      //   and high is the highest untested index
      //   At exit lowVac is the only vacancy and high is artificial

      // extend higher region - inlined to be more comparable
      int highVac = lowVac, // only vacancy in case no small data found
             // otherwise becomes vacancy beyond highest untested index
          curr = high;  // current index tested seeing if out of place
      while (curr > lowVac) {
        if (data[curr] < pivot) {
          data[lowVac] = data[curr];
          highVac = curr;
          break;
        }  
        curr--;
      }        
      // extend lower region - inlined to be more comparable
      curr = lowVac + 1; 
      lowVac = highVac; // only vacancy in case no large data found
         // otherwise again becomes vacancy before lowest untested index
      while (curr < highVac) {
        if (data[curr] >= pivot) {
          data[highVac] = data[curr];
          lowVac = curr;
          break;
        }  
        curr++;
      }
      high = highVac - 1; //artificial for exit if no misplaced value found
    }
    data[lowVac] = pivot;
    return lowVac; // final vacancy, same as highVac at this point
  }
}
