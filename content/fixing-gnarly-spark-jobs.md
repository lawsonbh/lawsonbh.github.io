Title: Fixing Gnarly Spark Queries Part 1
Date: 2022-08-02 20:34
Author: Bernie
Category: How Things Work

# Fixing Gnarly Spark Queries Part 1

As part of reflecting on work I've done, I wanted to start by sharing how I approach Spark Optimizations.

I have a love-hate relationship with Spark. You can process incredible amounts of data with it. You can also find yourself looking at hanging tasks, crazy compute costs, and silly storage situations. 

When someone called me in to help with a workload in Spark, I tried to really hone in on the following before starting any hands on coding work:

1. What's the objective?
2. What's the current baseline performance?
3. How do we know that any changes we introduce will still produce appropriate output? IE Do we know what correct-enough looks like?

The majority of the situations I was brought in to help with were focused on meeting some kind of time based deadline IE we need to speed this workload up!

Symptoms I looked at:

- What jobs are taking the longest?
- Within those jobs, do the stages make sense? 
- Within a stage, how well distributed are the tasks?

I solved the vast majority of issues just within the Spark UI. It's pretty great and I could do a whole post about it if there's interest!

All of this needs to be in the context of that objective you set up front with the work plan. 

A good portion of the workloads I've encountered can be addressed by taking a look at how often shuffles are occurring and how often executors are staying idle. 
If you are interested in a deep dive into any of these topics let me know!

