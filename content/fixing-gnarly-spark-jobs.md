<!DOCTYPE html>
<html lang="en">
<head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <meta name="generator" content="Pelican" />
        <title>Fixing Gnarly Spark Queries Part 1</title>
        <link rel="stylesheet" href="/theme/css/main.css" />
        <meta name="description" content="Fixing Gnarly Spark Queries Part 1 As part of reflecting on work I've done, I wanted to start by sharing how I approach Spark Optimizations. I..." />
</head>

<body id="index" class="home">
        <header id="banner" class="body">
                <h1><a href="/">Bernies Blog</a></h1>
                <nav><ul>
                    <li><a href="/pages/about.html">About</a></li>
                    <li class="active"><a href="/category/how-things-work.html">How Things Work</a></li>
                    <li><a href="/category/story.html">Story</a></li>
                </ul></nav>
        </header><!-- /#banner -->
<section id="content" class="body">
  <article>
    <header>
      <h1 class="entry-title">
        <a href="/fixing-gnarly-spark-queries-part-1.html" rel="bookmark"
           title="Permalink to Fixing Gnarly Spark Queries Part 1">Fixing Gnarly Spark Queries Part 1</a></h1>
    </header>

    <div class="entry-content">
<footer class="post-info">
        <abbr class="published" title="2022-08-02T20:34:00-04:00">
                Published: Tue 02 August 2022
        </abbr>

        <address class="vcard author">
                By                         <a class="url fn" href="/author/bernie.html">Bernie</a>
        </address>
<p>In <a href="/category/how-things-work.html">How Things Work</a>.</p>

</footer><!-- /.post-info -->      <h1>Fixing Gnarly Spark Queries Part 1</h1>
<p>As part of reflecting on work I've done, I wanted to start by sharing how I approach Spark Optimizations.</p>
<p>I have a love-hate relationship with Spark. You can process incredible amounts of data with it. You can also find yourself looking at hanging tasks, crazy compute costs, and silly storage situations. </p>
<p>When someone called me in to help with a workload in Spark, I tried to really hone in on the following before starting any hands on coding work:</p>
<ol>
<li>What's the objective?</li>
<li>What's the current baseline performance?</li>
<li>How do we know that any changes we introduce will still produce appropriate output? IE Do we know what correct-enough looks like?</li>
</ol>
<p>The majority of the situations I was brought in to help with were focused on meeting some kind of time based deadline IE we need to speed this workload up!</p>
<p>Symptoms I looked at:</p>
<ul>
<li>What jobs are taking the longest?</li>
<li>Within those jobs, do the stages make sense? </li>
<li>Within a stage, how well distributed are the tasks?</li>
</ul>
<p>I solved the vast majority of issues just within the Spark UI. It's pretty great and I could do a whole post about it if there's interest!</p>
<p>All of this needs to be in the context of that objective you set up front with the work plan. </p>
<p>A good portion of the workloads I've encountered can be addressed by taking a look at how often shuffles are occurring and how often executors are staying idle. 
If you are interested in a deep dive into any of these topics let me know!</p>
    </div><!-- /.entry-content -->

  </article>
</section>
        <section id="extras" class="body">
                <div class="blogroll">
                        <h2>links</h2>
                        <ul>
                            <li><a href="https://getpelican.com/">Pelican</a></li>
                            <li><a href="https://www.python.org/">Python.org</a></li>
                            <li><a href="https://palletsprojects.com/p/jinja/">Jinja2</a></li>
                        </ul>
                </div><!-- /.blogroll -->
                <div class="social">
                        <h2>social</h2>
                        <ul>

                            <li><a href="https://www.linkedin.com/in/bernardlawson/">LinkedIn</a></li>
                        </ul>
                </div><!-- /.social -->
        </section><!-- /#extras -->

        <footer id="contentinfo" class="body">
                <address id="about" class="vcard body">
                Proudly powered by <a href="https://getpelican.com/">Pelican</a>, which takes great advantage of <a href="https://www.python.org/">Python</a>.
                </address><!-- /#about -->

                <p>The theme is by <a href="https://www.smashingmagazine.com/2009/08/designing-a-html-5-layout-from-scratch/">Smashing Magazine</a>, thanks!</p>
        </footer><!-- /#contentinfo -->

</body>
</html>