Solr 7.7.2 fixes

Fixes:-
Read zk credentials from file for ZK ACLs(No Solr JIRA)
SOLR-13669
SOLR-13971
SOLR-14025
SOLR-12859
SOLR-13532
SOLR-13538
SOLR-13672 
SOLR-8346
SOLR-11763
SOLR-9515
SOLR-13342
SOLR-9515


* woodstox-core-asl-4.4.1.jar - This jar has been removed.
   Downside: Solr text tagger ("/tag") will still work but passing xmlOffsetAdjust=true will make no difference. This argument was used for passing xml input to the tagger and the jar used to mark/strip the xml before tagging.
