package cis5550.kvs;

import java.util.*;
import java.util.concurrent.*;
import java.net.*;
import java.io.*;
import cis5550.tools.HTTP;

public class KVSClient implements KVS {

  String coordinator;

  static class WorkerEntry implements Comparable<WorkerEntry> {
    String address;
    String id;

    WorkerEntry(String addressArg, String idArg) {
      address = addressArg;
      id = idArg;
    }

    public int compareTo(WorkerEntry e) {
      return id.compareTo(e.id);
    }
  };

  Vector<WorkerEntry> workers;
  boolean haveWorkers;

  public int numWorkers() throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.size();
  }

  public static String getVersion() {
    return "v1.4 Aug 5 2023";
  }

  public String getCoordinator() {
    return coordinator;
  }

  public String getWorkerAddress(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).address;
  }

  public String getWorkerID(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).id;
  }

  class KVSIterator implements Iterator<Row> {
    InputStream in;
    boolean atEnd;
    Row nextRow;
    int currentRangeIndex;
    String endRowExclusive;
    String startRow;
    String tableName;
    Vector<String> ranges;

    KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      in = null;
      currentRangeIndex = 0;
      atEnd = false;
      endRowExclusive = endRowExclusiveArg;
      tableName = tableNameArg;
      startRow = startRowArg;
      ranges = new Vector<String>();
      
      // Ensure workers are loaded before building ranges
      if (!haveWorkers) {
        downloadWorkers();
      }
      if (workers == null || workers.size() == 0) {
        throw new IOException("No KVS workers available - cannot create iterator for table: " + tableNameArg);
      }
      
      if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
        String url = getURL(tableNameArg, numWorkers()-1, startRowArg, ((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0))<0)) ? endRowExclusiveArg : getWorkerID(0));
        ranges.add(url);
      }
      for (int i=0; i<numWorkers(); i++) {
        if ((startRowArg == null) || (i == numWorkers()-1) || (startRowArg.compareTo(getWorkerID(i+1))<0)) {
          if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
            boolean useActualStartRow = (startRowArg != null) && (startRowArg.compareTo(getWorkerID(i))>0);
            boolean useActualEndRow = (endRowExclusiveArg != null) && ((i==(numWorkers()-1)) || (endRowExclusiveArg.compareTo(getWorkerID(i+1))<0));
            String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i), useActualEndRow ? endRowExclusiveArg : ((i<numWorkers()-1) ? getWorkerID(i+1) : null));
            ranges.add(url);
          }
        }
      }

      openConnectionAndFill();
    }

    protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      String params = "";
      if (startRowArg != null)
        params = "startRow="+startRowArg;
      if (endRowExclusiveArg != null)
        params = (params.equals("") ? "" : (params+"&"))+"endRowExclusive="+endRowExclusiveArg;
      return "http://"+getWorkerAddress(workerIndexArg)+"/data/"+tableNameArg+(params.equals("") ? "" : "?"+params);
    }

    void openConnectionAndFill() {
      try {
        if (in != null) {
          in.close();
          in = null;
        }

        if (atEnd)
          return;

        while (true) {
          if (currentRangeIndex >= ranges.size()) {
            atEnd = true;
            return;
          } 

          try {
            URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.connect();
            in = con.getInputStream();
            Row r = fill();
            if (r != null) {
              nextRow = r;
              break;
            }
          } catch (FileNotFoundException fnfe) {
          } catch (URISyntaxException use) {
          }

          currentRangeIndex ++;
        }
      } catch (IOException ioe) {
        if (in != null) {
          try { in.close(); } catch (Exception e) {}
          in = null;
        }
        atEnd = true;
      }
    }

    synchronized Row fill() {
      try {
        Row r = Row.readFrom(in);
        return r;
      } catch (Exception e) {
        return null;
      }
    }

    public synchronized Row next() {
      if (atEnd)
        return null;
      Row r = nextRow;
      nextRow = fill();
      while ((nextRow == null) && !atEnd) {
        currentRangeIndex ++;
        openConnectionAndFill();
      }
      
      return r;
    }

    public synchronized boolean hasNext() {
      return !atEnd;
    }
  }

  synchronized void downloadWorkers() throws IOException {
    String result = new String(HTTP.doRequest("GET", "http://"+coordinator+"/workers", null).body());
    String[] pieces = result.split("\n");
    int numWorkers = Integer.parseInt(pieces[0]);
    if (numWorkers < 1)
      throw new IOException("No active KVS workers");
    if (pieces.length != (numWorkers+1))
      throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
    workers.clear();
    for (int i=0; i<numWorkers; i++) {
      String[] pcs = pieces[1+i].split(",");
      workers.add(new WorkerEntry(pcs[1], pcs[0]));
    }
    Collections.sort(workers);

    haveWorkers = true;
  }

  synchronized int workerIndexForKey(String key) {
    if (workers == null || workers.isEmpty()) {
      throw new RuntimeException("Workers list is empty or null - cannot determine worker for key: " + key);
    }
    int chosenWorker = workers.size()-1;
    if (key != null) {
      for (int i=0; i<workers.size()-1; i++) {
        if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i+1).id) < 0))
          chosenWorker = i;
      }
    }

    return chosenWorker;
  }

  public KVSClient(String coordinatorArg) {
    coordinator = coordinatorArg;
    workers = new Vector<WorkerEntry>();
    haveWorkers = false;
  }

  public boolean rename(String oldTableName, String newTableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    boolean result = true;
    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("PUT", "http://"+w.address+"/rename/"+java.net.URLEncoder.encode(oldTableName, "UTF-8")+"/", newTableName.getBytes()).body();
        String res = new String(response);
        result &= res.equals("OK");
      } catch (Exception e) {}
    }

    return result;
  }

  public void delete(String oldTableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("PUT", "http://"+w.address+"/delete/"+java.net.URLEncoder.encode(oldTableName, "UTF-8")+"/", null).body();
        String result = new String(response);
      } catch (Exception e) {}
    }
  }

  public void put(String tableName, String row, String column, byte value[]) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    try {
      String target = "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8");
      byte[] response = HTTP.doRequest("PUT", target, value).body();
      String result = new String(response);
      if (!result.equals("OK")) 
      	throw new RuntimeException("PUT returned something other than OK: "+result+ "("+target+")");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    } 
  }


  public void cput(String tableName, String row, String column, byte value[], String ifColumn, String equalValue) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    try {
      String target = "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8") + "?ifcolumn=" + ifColumn + "&equal=" + equalValue;
      byte[] response = HTTP.doRequest("PUT", target, value).body();
      String result = new String(response);
      if (!result.equals("OK"))
        throw new RuntimeException("PUT returned something other than OK: "+result+ "("+target+")");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    }
  }

  public void put(String tableName, String row, String column, String value) throws IOException {
    put(tableName, row, column,value.getBytes());
  }

  public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    byte[] response = HTTP.doRequest("PUT", "http://"+workers.elementAt(workerIndexForKey(row.key())).address+"/data/"+tableName, row.toByteArray()).body();
    String result = new String(response);
    if (!result.equals("OK")) 
      throw new RuntimeException("PUT returned something other than OK: "+result);
  }

  /**
   * Bulk put multiple rows to KVS.
   * Groups rows by destination worker and sends in batches.
   * Uses the /data/:T/bulk endpoint for efficiency.
   */
  public void bulkPutRows(String tableName, List<Row> rows) throws IOException {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    
    if (!haveWorkers)
      downloadWorkers();

    // Group rows by destination worker
    Map<Integer, List<Row>> workerRows = new HashMap<>();
    for (Row row : rows) {
      int workerIdx = workerIndexForKey(row.key());
      workerRows.computeIfAbsent(workerIdx, k -> new ArrayList<>()).add(row);
    }

    // Send each batch to its destination worker
    for (Map.Entry<Integer, List<Row>> entry : workerRows.entrySet()) {
      int workerIdx = entry.getKey();
      List<Row> batch = entry.getValue();
      
      // Serialize all rows into one byte array
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      for (Row row : batch) {
        baos.write(row.toByteArray());
        baos.write('\n'); // Separator between rows
      }
      
      String url = "http://" + workers.elementAt(workerIdx).address + "/data/" + tableName + "/bulk";
      byte[] response = HTTP.doRequest("PUT", url, baos.toByteArray()).body();
      String result = new String(response);
      if (!result.startsWith("OK")) {
        throw new RuntimeException("Bulk PUT returned error: " + result);
      }
    }
  }

  /**
   * Bulk get multiple rows from KVS.
   * Groups keys by destination worker and sends batch requests in parallel.
   * Uses the /data/:T/bulkGet endpoint for efficiency.
   * 
   * @param tableName The table to read from
   * @param rowKeys List of row keys to fetch
   * @return Map of rowKey -> Row (Row is null if not found in the table)
   */
  public Map<String, Row> bulkGetRows(String tableName, List<String> rowKeys) throws IOException {
    if (rowKeys == null || rowKeys.isEmpty()) {
      return Collections.emptyMap();
    }
    
    if (!haveWorkers)
      downloadWorkers();

    // Group keys by destination worker
    Map<Integer, List<String>> workerKeys = new HashMap<>();
    for (String key : rowKeys) {
      int workerIdx = workerIndexForKey(key);
      workerKeys.computeIfAbsent(workerIdx, k -> new ArrayList<>()).add(key);
    }

    // Results map (thread-safe for parallel writes)
    Map<String, Row> results = new ConcurrentHashMap<>();
    
    // Send requests in parallel to each worker
    List<Thread> threads = new ArrayList<>();
    List<Exception> errors = Collections.synchronizedList(new ArrayList<>());
    
    for (Map.Entry<Integer, List<String>> entry : workerKeys.entrySet()) {
      final int workerIdx = entry.getKey();
      final List<String> keys = entry.getValue();
      
      Thread t = new Thread(() -> {
        try {
          // Build request body: newline-separated keys
          String body = String.join("\n", keys);
          String url = "http://" + workers.elementAt(workerIdx).address + "/data/" + tableName + "/bulkGet";
          HTTP.Response resp = HTTP.doRequest("POST", url, body.getBytes("UTF-8"));
          
          if (resp.statusCode() != 200) {
            errors.add(new IOException("Bulk GET returned status " + resp.statusCode() + " from worker " + workerIdx));
            return;
          }
          
          // Parse response - rows separated by newlines
          ByteArrayInputStream bais = new ByteArrayInputStream(resp.body());
          for (String key : keys) {
            try {
              Row row = Row.readFrom(bais);
              if (row != null && row.columns().size() > 0) {
                // Row has data - store it
                results.put(key, row);
              }
              // If row is null or has no columns, key was not found - don't add to results
              
              // Skip newline separator
              if (bais.available() > 0) {
                bais.read();
              }
            } catch (Exception e) {
              // Parsing error for this key - skip it
            }
          }
        } catch (Exception e) {
          errors.add(e);
        }
      });
      threads.add(t);
      t.start();
    }
    
    // Wait for all threads to complete
    for (Thread t : threads) {
      try { 
        t.join(); 
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for bulk get", e);
      }
    }
    
    // If all workers failed, throw an exception
    if (!errors.isEmpty() && results.isEmpty()) {
      throw new IOException("Bulk GET failed: " + errors.get(0).getMessage());
    }
    
    return results;
  }

  public Row getRow(String tableName, String row) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response resp = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    if (resp.statusCode() == 404)
      return null;

    byte[] result = resp.body();
    try {
      return Row.readFrom(new ByteArrayInputStream(result));
    } catch (Exception e) {
      throw new RuntimeException("Decoding error while reading Row from getRow() URL");
    }
  }

  public byte[] get(String tableName, String row, String column) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response res = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8"), null);
    return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
  }

  public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response r = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    return r.statusCode() == 200;
  }

  public int count(String tableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    int total = 0;
    for (WorkerEntry w : workers) {
      HTTP.Response r = HTTP.doRequest("GET", "http://"+w.address+"/count/"+tableName, null);
      if ((r != null) && (r.statusCode() == 200)) {
        String result = new String(r.body());
        total += Integer.valueOf(result).intValue();
      }
    } 
    return total;
  }

/*  public void persist(String tableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    for (WorkerEntry w : workers) 
      HTTP.doRequest("PUT", "http://"+w.address+"/persist/"+tableName, null);
  } */

  public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
    return scan(tableName, null, null);
  }

  public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    return new KVSIterator(tableName, startRow, endRowExclusive);
  }

  public static void main(String args[]) throws Exception {
  	if (args.length < 2) {
      System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
      System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
      System.err.println("Syntax: client <coordinator> cput <tableName> <row> <column> <value> <ifColumn> <equalValue>");
      System.err.println("Syntax: client <coordinator> scan <tableName>");
      System.err.println("Syntax: client <coordinator> delete <tableName>");
      System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
  		System.exit(1);
  	}

  	KVSClient client = new KVSClient(args[0]);
    if (args[1].equals("put")) {
    	if (args.length != 6) {
	  		System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
	  		System.exit(1);
    	}
      client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
    } else if (args[1].equals("cput")) {
      if (args.length != 8) {
        System.err.println("Syntax: client <coordinator> put <tableName> <row> <column> <value> <ifColumn> <equalValue>");
        System.exit(1);
      }
      client.cput(args[2], args[3], args[4], args[5].getBytes("UTF-8"), args[6], args[7]);
    } else if (args[1].equals("get")) {
      if (args.length != 5) {
        System.err.println("Syntax: client <coordinator> get <tableName> <row> <column>");
        System.exit(1);
      }
      byte[] val = client.get(args[2], args[3], args[4]);
      if (val == null)
        System.err.println("No value found");
      else 
        System.out.write(val);
    } else if (args[1].equals("scan")) {
      if (args.length != 3) {
        System.err.println("Syntax: client <coordinator> scan <tableName>");
        System.exit(1);
      }

      Iterator<Row> iter = client.scan(args[2], null, null);
      int count = 0;
      while (iter.hasNext()) {
        System.out.println(iter.next());
        count ++;
      }
      System.err.println(count+" row(s) scanned");
    } else if (args[1].equals("delete")) {
      if (args.length != 3) {
        System.err.println("Syntax: client <coordinator> delete <tableName>");
        System.exit(1);
      }

      client.delete(args[2]);
      System.err.println("Table '"+args[2]+"' deleted");
    } else if (args[1].equals("rename")) {
      if (args.length != 4) {
        System.err.println("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
        System.exit(1);
      }
      if (client.rename(args[2], args[3]))
        System.out.println("Success");
      else
        System.out.println("Failure");
    } else {
    	System.err.println("Unknown command: "+args[1]);
    	System.exit(1);
    }
  }
};