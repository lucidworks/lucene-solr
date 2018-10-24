Buildfile: /Users/Erick/apache/solr5_3_2_fieldcache/build.xml

test-help:

test-help:
     [help] 
     [help] #
     [help] # Test case filtering. --------------------------------------------
     [help] #
     [help] # - 'tests.class' is a class-filtering shell-like glob pattern,
     [help] #   'testcase' is an alias of "tests.class=*.${testcase}"
     [help] # - 'tests.method' is a method-filtering glob pattern.
     [help] #   'testmethod' is an alias of "tests.method=${testmethod}*"
     [help] #
     [help] 
     [help] # Run a single test case (variants)
     [help] ant test -Dtests.class=org.apache.lucene.package.ClassName
     [help] ant test "-Dtests.class=*.ClassName"
     [help] ant test -Dtestcase=ClassName
     [help] 
     [help] # Run all tests in a package and sub-packages
     [help] ant test "-Dtests.class=org.apache.lucene.package.*"
     [help] 
     [help] # Run any test methods that contain 'esi' (like: ...r*esi*ze...).
     [help] ant test "-Dtests.method=*esi*"
     [help] 
     [help] #
     [help] # Seed and repetitions. -------------------------------------------
     [help] #
     [help] 
     [help] # Run with a given seed (seed is a hex-encoded long).
     [help] ant test -Dtests.seed=DEADBEEF
     [help] 
     [help] # Repeats _all_ tests of ClassName N times. Every test repetition
     [help] # will have a different seed.
     [help] ant test -Dtests.iters=N -Dtestcase=ClassName
     [help] 
     [help] # Repeats _all_ tests of ClassName N times. Every test repetition
     [help] # will have exactly the same master (dead) and method-level (beef)
     [help] # seed.
     [help] ant test -Dtests.iters=N -Dtestcase=ClassName -Dtests.seed=dead:beef
     [help] 
     [help] # Repeats a given test N times (note the filters - individual test
     [help] # repetitions are given suffixes, ie: testFoo[0], testFoo[1], etc...
     [help] # so using testmethod or tests.method ending in a glob is necessary
     [help] # to ensure iterations are run).
     [help] ant test -Dtests.iters=N -Dtestcase=ClassName -Dtestmethod=mytest
     [help] ant test -Dtests.iters=N -Dtestcase=ClassName -Dtests.method=mytest*
     [help] 
     [help] # Repeats N times but skips any tests after the first failure or M
     [help] # initial failures.
     [help] ant test -Dtests.iters=N -Dtests.failfast=yes -Dtestcase=...
     [help] ant test -Dtests.iters=N -Dtests.maxfailures=M -Dtestcase=...
     [help] 
     [help] # Repeats every suite (class) and any tests inside N times
     [help] # can be combined with -Dtestcase or -Dtests.iters, etc.
     [help] # Can be used for running a single class on multiple JVMs
     [help] # in parallel.
     [help] ant test -Dtests.dups=N ...
     [help] 
     [help] # Test beasting: Repeats every suite with same seed per class
     [help] # (N times in parallel) and each test inside (M times). The whole
     [help] # run is repeated (beasting) P times in a loop, with a different
     [help] # master seed. You can combine beasting with any other parameter,
     [help] # just replace "test" with "beast" and give -Dbeast.iters=P
     [help] # (P >> 1).
     [help] ant beast -Dtests.dups=N -Dtests.iters=M -Dbeast.iters=P \
     [help]   -Dtestcase=ClassName
     [help] 
     [help] #
     [help] # Test groups. ----------------------------------------------------
     [help] #
     [help] # test groups can be enabled or disabled (true/false). Default
     [help] # value provided below in [brackets].
     [help] 
     [help] ant -Dtests.nightly=[false]   - nightly test group (@Nightly)
     [help] ant -Dtests.weekly=[false]    - weekly tests (@Weekly)
     [help] ant -Dtests.awaitsfix=[false] - known issue (@AwaitsFix)
     [help] ant -Dtests.slow=[true]       - slow tests (@Slow)
     [help] 
     [help] #
     [help] # Load balancing and caches. --------------------------------------
     [help] #
     [help] 
     [help] # Run sequentially (one slave JVM).
     [help] ant -Dtests.jvms=1 test
     [help] 
     [help] # Run with more slave JVMs than the default.
     [help] # Don't count hypercores for CPU-intense tests.
     [help] # Make sure there is enough RAM to handle child JVMs.
     [help] ant -Dtests.jvms=8 test
     [help] 
     [help] # Use repeatable suite order on slave JVMs (disables job stealing).
     [help] ant -Dtests.dynamicAssignmentRatio=0 test
     [help] 
     [help] # Update global (versioned!) execution times cache (top level).
     [help] ant clean test
     [help] ant -f lucene/build.xml test-updatecache
     [help] 
     [help] #
     [help] # Miscellaneous. --------------------------------------------------
     [help] #
     [help] 
     [help] # Run all tests without stopping on errors (inspect log files!).
     [help] ant -Dtests.haltonfailure=false test
     [help] 
     [help] # Run more verbose output (slave JVM parameters, etc.).
     [help] ant -verbose test
     [help] 
     [help] # Include additional information like what is printed to 
     [help] # sysout/syserr, even if the test passes.
     [help] # Enabled automatically when running for a single test case.
     [help] ant -Dtests.showSuccess=true test
     [help] 
     [help] # Change the default suite timeout to 5 seconds.
     [help] ant -Dtests.timeoutSuite=5000! ...
     [help] 
     [help] # Display local averaged stats, if any (30 slowest tests).
     [help] ant test-times -Dmax=30
     [help] 
     [help] # Display a timestamp alongside each suite/ test.
     [help] ant -Dtests.timestamps=on ...
     [help] 
     [help] # Override forked JVM file.encoding
     [help] ant -Dtests.file.encoding=XXX ...
     [help] 
     [help] # Don't remove any temporary files under slave directories, even if
     [help] # the test passes (any of the following props):
     [help] ant -Dtests.leaveTemporary=true
     [help] ant -Dtests.leavetmpdir=true
     [help] ant -Dsolr.test.leavetmpdir=true
     [help] 
     [help] # Do *not* filter stack traces emitted to the console.
     [help] ant -Dtests.filterstacks=false
     [help] 
     [help] # Skip checking for no-executed tests in modules
     [help] ant -Dtests.ifNoTests=ignore ...
     [help] 
     [help] # Output test files and reports.
     [help] ${tests-output}/tests-report.txt    - full ASCII tests report
     [help] ${tests-output}/tests-failures.txt  - failures only (if any)
     [help] ${tests-output}/tests-report-*      - HTML5 report with results
     [help] ${tests-output}/junit4-*.suites     - per-JVM executed suites
     [help]                                       (important if job stealing).
     [help]       

BUILD SUCCESSFUL
Total time: 0 seconds
