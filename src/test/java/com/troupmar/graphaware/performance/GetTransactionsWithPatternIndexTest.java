package com.troupmar.graphaware.performance;

import com.esotericsoftware.minlog.Log;
import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import com.troupmar.graphaware.CypherQuery;
import com.troupmar.graphaware.PatternIndex;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.*;
import java.util.*;

public class GetTransactionsWithPatternIndexTest implements PerformanceTest {

    private final String query = "MATCH (a)-[r]-(b)-[s]-(c)-[t]-(d)-[u]-(e)-[v]-(c)-[w]-(a)-[x]-(d)-[y]-(b)-[z]-(e)-[z2]-(a) " +
            "RETURN a, b, c, d, e";
    private final String pattern = "(a)-[r]-(b)-[s]-(c)-[t]-(d)-[u]-(e)-[v]-(c)-[w]-(a)-[x]-(d)-[y]-(b)-[z]-(e)-[z2]-(a)";
    private final String indexName = "transaction-index";

    private PatternIndexModel model;

    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "GetTransactionsByPatternQuery";
    }

    @Override
    public String longName() {
        return "Pattern query to get all transaction patterns.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        //result.add(new ObjectParameter("cache", new HighLevelCache()));
        //result.add(new ObjectParameter("cache", new LowLevelCache()));
        //result.add(new ObjectParameter("cache", new NoCache()));
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 10 : 5;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 5;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> databaseParameters(Map<String, Object> params) {
        Map<String, String> config = ((CacheConfiguration) params.get("cache")).addToConfig(Collections.<String, String>emptyMap());
        config.put("com.graphaware.runtime.enabled", "true");
        config.put("com.graphaware.module.patternIndex.1", "com.troupmar.graphaware.transactionHandle.TransactionHandleBootstrapper");
        return config;
    }




    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareDatabase(GraphDatabaseService database, final Map<String, Object> params) {
        PatternIndexModel.destroy();
        model = PatternIndexModel.getInstance(database);

        /*
        try {
            PatternQuery patternQuery = new PatternQuery(pattern, database);
            model.buildNewIndex(patternQuery, indexName);
            Log.info("Index created");

        } catch (InvalidCypherMatchException e) {
            e.printStackTrace();
        } catch (PatternIndexAlreadyExistsException e) {
            e.printStackTrace();
        }

        // CREATING DATABASE
        //createNewTransactionDb(database);

        // LOADING DATABASE FROM FILE
        //loadTransactionDbFromFile(database, "export-nodes.cypher", "export-rels.cypher");
        */
    }

    public String getExistingDatabasePath() {
        return "testDb/transactions-indexed.db.zip";
        //return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.AFTER_PARAM_CHANGE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long run(final GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                try {
                    CypherQuery cq = new CypherQuery(query, database);
                    model.getResultFromIndex(cq, indexName);
                } catch (InvalidCypherException e) {
                    e.printStackTrace();
                } catch (InvalidCypherMatchException e) {
                    e.printStackTrace();
                } catch (PatternIndexNotFoundException e) {
                    e.printStackTrace();
                }
            }
        });

        return time;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        throw new UnsupportedOperationException("never needed, database rebuilt after every param change");
    }

    private void createNewTransactionDb(GraphDatabaseService database) {
        database.execute("WITH [\"1001\",\"1002\",\"1003\",\"1004\",\"1004\",\"1005\",\"1006\",\"1007\",\"1007\",\"1008\"] AS names\n" +
                "FOREACH (r IN range(0,300) | CREATE (:Cart {id:r, number:names[r % size(names)]+\" \"+r}));");

        Log.info("NODES ADDED");

        database.execute("match (c:Cart),(a:Cart)\n" +
                "with c,a\n" +
                "limit 90000\n" +
                "where rand() < 0.08\n" +
                "create (c)-[:TRANSACTION]->(a);");

        Log.info("RELS ADDED");

        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        Log.info("DB INDEX SIZE: " + model.getNumOfUnitsInPatternIndex(patternIndex));
        Log.info("DB CREATED");
    }

    private void loadTransactionDbFromFile(GraphDatabaseService database, String nodesInFile, String relsInFile) {
        String queryString = "";
        int i = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(relsInFile))) {
            String nodesToCreate = new Scanner( new File(nodesInFile) ).useDelimiter("\\\\A").next();

            String line;
            queryString = nodesToCreate + " ";
            while ((line = br.readLine()) != null) {
                queryString += line + " ";
                if (i % 500 == 0) {
                    database.execute(queryString);
                    queryString = nodesToCreate + " ";
                }
                i++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        database.execute(queryString);

        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        Log.info("DB INDEX SIZE: " + model.getNumOfUnitsInPatternIndex(patternIndex));
        Log.info("DB CREATED");
    }

}