# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps
# MAGIC ### Data Value Catalyst Data Quality and Rules Engine
# MAGIC 
# MAGIC 
# MAGIC Source: The DataOps Cookbook Second Edition. Christopher Bergh, Gil Benghiat, Eran Strod

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Data Value Catalyst Foundations
# MAGIC 
# MAGIC <img src="https://eesharedpublic.blob.core.windows.net/public/ktk Foundations.png" width=400>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Eight Challenges of Data Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC 1. The Goalposts Keep Moving
# MAGIC > *Users are not data experts; usually don't know what they want until we show them.
# MAGIC Immediate response is expected. They need everything ASAP.
# MAGIC Analytics often generates more questions than answers.*
# MAGIC 2. Data Lives in Silos
# MAGIC > *Integrating from myriad sources is a major undertaking.
# MAGIC Gaining access, plan, implement architectural changes, develop/test/deploy new analytics.
# MAGIC Desire for fast answers is belied by complex, lengthy process subject to bottlenecks and blockages.*
# MAGIC 3. Data Formats are not Optimized
# MAGIC > *Data is almost always in the wrong format and often does not make intuitive sense.*
# MAGIC 4. Data Errors
# MAGIC > *Data is a naturally occurring phenomenon and will eventually contain errors.*
# MAGIC 5. Bad Data Ruins Good Reports
# MAGIC > *Unhandled data errors lead to internal stakeholders becoming dissatisfied and erodes the credibility of the data analytics team.
# MAGIC Requires unplanned work to fix, diverting from high priority projects.*
# MAGIC 6. Data Pipeline Maintenance Never Ends
# MAGIC > *Updating data sources, schema enhancements, analytics improvements etc. trigger pipeline updates.
# MAGIC Continuous changes and improvements require validation and verification (unappreciated when viewed against the growing backlog).
# MAGIC Data teams can spend up to 80% of their time updating, maintaining and assuring the quality of the data pipeline.*
# MAGIC 7. Manual Process Fatigue
# MAGIC > *Data integration, cleansing, transformation, quality assurance and deployment of new analytics must be performed flawlessly every day.
# MAGIC Teams perform numerous manual processes regularly.
# MAGIC Rote procedures are error prone, time consuming and tedious, which leads to burnout.*
# MAGIC 8. The Trap of "Hope, Heroism and Caution"
# MAGIC > *Forrester Research: 60% of data and analytics decision makers are **NOT CONFIDENT** in their analytics insights.
# MAGIC Only 10% of organizations sufficiently manage the quality of data and analytics.
# MAGIC Only 16% believe they perform well in producing accurate models.
# MAGIC Teams respond in one of three ways:*
# MAGIC >> **Heroism**: *Work long hours to compensate for the gap between performance and expectations.
# MAGIC Not sustainable or scalable.
# MAGIC Resets expectations without providing additional resources.*
# MAGIC **Hope**: *Do minimal testing, push it out and **hope** it does not break.
# MAGIC When data errors inevitably happen, upset users, loss of credibility.*
# MAGIC **Caution**: *Give each data-analytics project a longer dev and test schedule.
# MAGIC deliver higher quality but fewer, slower features to users.
# MAGIC viewed as slow, bureaucratic and inefficient*

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is Data Ops?

# COMMAND ----------

# MAGIC %md
# MAGIC Learning how to create and publish analytics in a new way.
# MAGIC 
# MAGIC <img src="https://eesharedpublic.blob.core.windows.net/public/DataOps.png" width=900>
# MAGIC 
# MAGIC Combination of tools and methods which streamline the development of new analytics while ensuring impeccable data quality.
# MAGIC Helps shorten the cycle time for producing analytic value and innovation while avoiding the trap of "Hope, Heroism and Caution".
# MAGIC 
# MAGIC <img src="https://eesharedpublic.blob.core.windows.net/public/DataOpsInfluences.png" width=400>
# MAGIC 
# MAGIC Whether in Data Science, Data Engineering, Data Management, Business Intelligence:
# MAGIC **Value**:
# MAGIC * Individuals and interactions over processes and tools
# MAGIC * Working analytics over comprehensive documentation
# MAGIC * Customer collaboration over contract negotiation
# MAGIC * Experimentation, iteration, and feedback over extensive upfront design
# MAGIC * Cross-functional ownership of operations over siloed responsibilities
# MAGIC 
# MAGIC 
# MAGIC **DataOps Principles:**
# MAGIC 1. Continually Satisfy your customer
# MAGIC 2. Value Working Analytics
# MAGIC 3. Embrace Change
# MAGIC 4. It's a Team Sport
# MAGIC 5. Daily Interactions
# MAGIC 6. Self-Organize
# MAGIC 7. Reduce Heroism
# MAGIC 8. Reflect
# MAGIC 9. Analytics is Code
# MAGIC 10. Orchestrate
# MAGIC 11. Make it Reproducible
# MAGIC 12. Disposable Environments
# MAGIC 13. Simplicity
# MAGIC 14. Analytics is Manufacturing
# MAGIC 15. Quality is Paramount
# MAGIC 16. Monitor Quality and Performance
# MAGIC 17. Reuse
# MAGIC 18. Improve Cycle Times

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seven Steps to Implement DataOps

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Add Data and Logic Tests
# MAGIC > *Suite of tests to measure quality without requiring time consuming manual testing
# MAGIC Every time a data-analytics team member makes a change, he or she adds a test for that change.
# MAGIC Testing is added incrementally, with the addition of each feature.
# MAGIC Testing gradually improves and quality is literally built in.
# MAGIC Allows you to move fast, make changes quickly, rerun the suite to regression test.
# MAGIC Analogous to Statistical process control in manufacturing ('stop the line')*
# MAGIC 2. Use a Version Control System
# MAGIC > *scripts, source code, algorithms, html, configuration files, parameter files, containers etc. are all just **code** and need to be in versioned source control.
# MAGIC Organize and manage changes and revisions to code.
# MAGIC Facilitates disaster recovery
# MAGIC Allows for branching and merging*
# MAGIC 3. Branch and Merge
# MAGIC > *Feature development is done on a local copy (branch)
# MAGIC Work on many coding changes to the data-analytics pipeline in parallel
# MAGIC Run own tests, make changes, experiment, discard, roll back  *
# MAGIC 4. Use Multiple Environments
# MAGIC > *Each team member has own development tools, own private copy of the source code while staying coordinated with the rest of the team
# MAGIC Create individual development environments with own representative data
# MAGIC Isolates the rest of the organization from being impacted by their work*
# MAGIC 5. Reuse and Containerize
# MAGIC > *Break pipelines into smaller components
# MAGIC Segmented or containerized.
# MAGIC Single responsibility pipelines*
# MAGIC 6. Parameterize Your Processing
# MAGIC > *Robust pipeline design allows engineer to specify options using parameters.
# MAGIC Flexibility to accommodate different run-time circumstances  *
# MAGIC 7. Work Without Fear or Heroism
# MAGIC > *Relax because quality is assured without resorting to fear or heroism
# MAGIC Automate pipeline for continuous deployment
# MAGIC Simplify release of new enhancements
# MAGIC Enabling the team to focus on the next valuable feature*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process

# COMMAND ----------

# MAGIC %md
# MAGIC * Value Pipeline: DataOps Production Orchestrates Automated Testing, Monitoring and Statistical Process Control
# MAGIC * Innovation Pipeline: Test and Verify new analytics before deploying into production.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://eesharedpublic.blob.core.windows.net/public/DataOpsProcess.png" width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://eesharedpublic.blob.core.windows.net/public/DataOpsDataAndCodeTesting.png" width=600>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Value Catalyst - Data Quality Framework

# COMMAND ----------

# MAGIC %md
# MAGIC Perform validation at the end of each major stage of the pipeline.
# MAGIC 
# MAGIC * Phase Gate
# MAGIC * Data Quality Scoring is input into data quality routing
# MAGIC 
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/dqframework.png" width=900>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://ktkdmdpe16public.blob.core.windows.net/public/dqvaluepipeline.png" width=900>
