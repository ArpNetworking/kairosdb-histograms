KairosDB Histograms
===================

Histograms are a method of storing multiple samples in a compact way that allows for 
reaggregation of most statistics (with a loss of precision).  This plugin creates a histogram 
datapoint type and recreates many of the aggregators that are used in stock KairosDB.  In
addition, it adds 3 new aggregators: merge, moving window and apdex.

Merge
-----

Computes a merged histogram from multiple histograms.  Bucket values are as precise as the least-precise histogram. Supports histograms.

Apdex
-----

Computes an Apdex score from the target argument. Supports histograms.

Moving Window
-------------

Helper aggregator that allows a moving window computation from any other aggregator.  Supports histograms and scalar.
