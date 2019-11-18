package atlas.janusgraph.example;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.janus.serializer.TypeCategorySerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;


import org.apache.atlas.repository.graphdb.janus.serializer.BigDecimalSerializer;
import org.apache.atlas.repository.graphdb.janus.serializer.BigIntegerSerializer;
import org.apache.commons.configuration.Configuration;

import static org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase.GRAPH_PREFIX;
import static org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase.SOLR_ZOOKEEPER_URL;
import static org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase.SOLR_ZOOKEEPER_URLS;
import static org.apache.commons.configuration.ConfigurationConverter.getConfiguration;


public class AtlasJanus {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtlasJanus.class);


    public static Configuration getConfig() throws AtlasException {
       Configuration configProperties = ApplicationProperties.get();


        configProperties.setProperty(SOLR_ZOOKEEPER_URLS, configProperties.getStringArray(SOLR_ZOOKEEPER_URL));
        Configuration janusConfig = ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);



        //add serializers for non-standard property value types that Atlas uses
        janusConfig.addProperty("attributes.custom.attribute1.attribute-class", TypeCategory.class.getName());
        janusConfig.addProperty("attributes.custom.attribute1.serializer-class", TypeCategorySerializer.class.getName());

        //not ideal, but avoids making large changes to Atlas
        janusConfig.addProperty("attributes.custom.attribute2.attribute-class", ArrayList.class.getName());
        janusConfig.addProperty("attributes.custom.attribute2.serializer-class", SerializableSerializer.class.getName());

        janusConfig.addProperty("attributes.custom.attribute3.attribute-class", BigInteger.class.getName());
        janusConfig.addProperty("attributes.custom.attribute3.serializer-class", BigIntegerSerializer.class.getName());

        janusConfig.addProperty("attributes.custom.attribute4.attribute-class", BigDecimal.class.getName());
        janusConfig.addProperty("attributes.custom.attribute4.serializer-class", BigDecimalSerializer.class.getName());

        return janusConfig;
    }




    public void managementApi(JanusGraph graph) {
        final JanusGraphManagement management = graph.openManagement();
        try {
//            // naive check if the schema was previously created
//            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
//                management.rollback();
//                return;
//            }
//            LOGGER.info("creating schema");
//            createProperties(management);
//            createVertexLabels(management);
//            createEdgeLabels(management);
//            createCompositeIndexes(management);
//            createMixedIndexes(management);
//            management.commit();

            //https://developer.ibm.com/articles/apache-atlas-and-janusgraph-graph-based-meta-data-management/
            management.get("storage.hbase.table");
            management.get("index.search.backend");
            management.get("index.search.index-name");

        } catch (Exception e) {
            management.rollback();
        }
    }

    public static void main(String[] args) {
        try {
            Configuration config = getConfig();
            System.out.println("================== Starting janus object >=======================");

            AtlasGraphQuery query = AtlasGraphProvider
                    .getGraphInstance().query().has("__typeName", "hive_table").has("owner", "root")
                    .has("__state", "ACTIVE");

            System.out.println("================== Started janus object =======================");


//            JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-hbase-solr.properties");
//            //JanusGraph graph = JanusGraphFactory.open(getConfiguration(config));
//
//            AtlasJanus jg = new AtlasJanus();
//            jg.managementApi(graph);
//
//            GraphTraversalSource g = graph.traversal();
//            g.close();

        } catch (Exception e) {
            System.out.println(e);

        }
        System.exit(0);
    }
}
