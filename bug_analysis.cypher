
// LOAD PROJECT NODES

LOAD CSV WITH HEADERS FROM "file:///bugreport2.csv" AS Projects
MERGE (p:Project {
    projectname: Projects.Project_Name
});

// LOAD COMPONENT NODES

LOAD CSV WITH HEADERS FROM "file:///bugreport2.csv" AS Projects
MERGE (c:Component {
    cprojectname: Projects.Project_Name,
    compname: Projects.Component,
    compid: Projects.Comp_ID
});

// LOAD BUG NODES

LOAD CSV WITH HEADERS FROM "file:///bugreport2.csv" AS Projects
MERGE (b:Bug {
    bugid: Projects.Bug_ID,
    bcompname: Projects.Component,
    bugtype: Projects.Bug_Type,
    priority: Projects.Priority,
    eng: Projects.Bug_Engineer,
    bcompid: Projects.Comp_ID
});

// LOAD ENGINEER NODES

LOAD CSV WITH HEADERS FROM "file:///bugreport2.csv" AS Projects
MERGE (a:Engineer {
    bugeng: Projects.Bug_Engineer,
    engid: Projects.Eng_ID
});

// CREATE RELATIONSHIPS

// Project -> Component

MATCH (p:Project)
MATCH (c:Component)
WHERE p.projectname = c.cprojectname
MERGE (p)-[:LINK {weight:0.1}]->(c);

// Component -> Bug

MATCH (c:Component)
MATCH (b:Bug)
WHERE c.compid = b.bcompid
MERGE (c)-[:LINK {weight:0.1}]->(b);

// Bug -> Engineer

MATCH (b:Bug)
MATCH (a:Engineer)
WHERE b.eng = a.bugeng
MERGE (b)-[:LINK {weight:0.1}]->(a);

// PAGERANK

CALL gds.graph.drop('PageRankGraph', false);

CALL gds.graph.project(
    'PageRankGraph',
    '*',
    'LINK'
);

CALL gds.pageRank.stream(
    'PageRankGraph',
    {
        maxIterations:4,
        dampingFactor:0.85
    }
)
YIELD nodeId, score
RETURN
gds.util.asNode(nodeId).bugeng AS Eng_Name,
score AS Ranking
ORDER BY Ranking DESC
LIMIT 10;

// DEGREE CENTRALITY

CALL gds.graph.drop('centralitygraph', false);

CALL gds.graph.project(
    'centralitygraph',
    '*',
    {
        LINK:{
            orientation:'REVERSE',
            properties:'weight'
        }
    }
);

CALL gds.degree.stream('centralitygraph')
YIELD nodeId, score
RETURN
gds.util.asNode(nodeId).bugeng AS Eng_Name,
score AS Centrality
ORDER BY Centrality DESC
LIMIT 10;

// LOUVAIN COMMUNITY DETECTION

CALL gds.graph.drop('communitygraph', false);

CALL gds.graph.project(
    'communitygraph',
    '*',
    {
        LINK:{
            orientation:'UNDIRECTED',
            properties:'weight'
        }
    }
);

CALL gds.louvain.stream('communitygraph')
YIELD nodeId, communityId
RETURN
COUNT(*) AS Frequency,
gds.util.asNode(nodeId).bugeng AS Eng_Name,
communityId
ORDER BY communityId;

//SIMILARITY OF BUGS

// FASTRP EMBEDDINGS

CALL gds.graph.drop('similaritygraph', false);

CALL gds.graph.project(
    'similaritygraph',
    '*',
    {
        LINK:{
            orientation:'REVERSE',
            properties:'weight'
        }
    }
);

CALL gds.fastRP.mutate(
    'similaritygraph',
    {
        embeddingDimension:4,
        randomSeed:42,
        mutateProperty:'embedding',
        relationshipWeightProperty:'weight',
        iterationWeights:[0.1,0.1,0.1,0.1]
    }
)
YIELD nodePropertiesWritten;

// KNN SIMILARITY

CALL gds.knn.write(
    'similaritygraph',
    {
        topK:2,
        nodeProperties:['embedding'],
        randomSeed:42,
        concurrency:1,
        sampleRate:1.0,
        deltaThreshold:0.0,
        writeRelationshipType:'SIMILAR',
        writeProperty:'weight'
    }
)
YIELD
nodesCompared,
relationshipsWritten,
similarityDistribution
RETURN
nodesCompared,
relationshipsWritten,
similarityDistribution.mean AS meanSimilarity;

// FIND SIMILAR BUGS

MATCH (n:Bug)-[r:SIMILAR]->(m:Bug)
WHERE n.bugtype = m.bugtype
AND n.priority = m.priority
RETURN
n.bugid AS Bug1,
m.bugid AS Bug2,
r.weight AS Similarity,
n.bugtype AS BugType,
n.priority AS Priority
ORDER BY Similarity DESC, Bug1, Bug2
LIMIT 10;
