package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          PageParserFactory parserFactory,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls  ) {
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
      this.maxDepth = maxDepth;
      this.ignoredUrls = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Set<String> urlsVisited = Collections.synchronizedSet(new HashSet<>());
    Map<String, Integer> wordCounts = Collections.synchronizedMap(new HashMap<>());
    Instant endAt = clock.instant().plus(timeout);

    for (String url: startingUrls) {
      pool.invoke(
              new WebCrawlAction(
                      url,
                      maxDepth,
                      endAt,
                      wordCounts,
                      urlsVisited,
                      parserFactory,
                      clock,
                      ignoredUrls));
    }

    if (!wordCounts.isEmpty()) return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(wordCounts, popularWordCount))
            .setUrlsVisited(urlsVisited.size())
            .build();

    return new CrawlResult.Builder()
            .setWordCounts(wordCounts)
            .setUrlsVisited(urlsVisited.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

 class WebCrawlAction extends RecursiveAction {
   private final String url;
   private final int maxVisitDepth;
   private final Instant deadline;
   private Map<String, Integer> wordCounts;
   private Set<String> urlsVisited;
   private final PageParserFactory parserFactory;
   private final Clock clock;
   private final List<Pattern> ignoredUrls;

   public WebCrawlAction(String url, int maxVisitDepth, Instant deadline, Map<String, Integer> wordCounts, Set<String> urlsVisited,
                         PageParserFactory parserFactory,
                         Clock clock,
                         @IgnoredUrls List<Pattern> ignoredUrls
   ) {
     this.url = url;
     this.deadline = deadline;
     this.maxVisitDepth = maxVisitDepth;
     this.wordCounts = wordCounts;
     this.urlsVisited = urlsVisited;
     this.parserFactory = parserFactory;
     this.clock = clock;
     this.ignoredUrls = ignoredUrls;
   }

   @Override
   protected void compute() {
     for (Pattern p : ignoredUrls) if (p.matcher(url).matches()) return;
     if (clock.instant().isAfter(deadline) || !(maxVisitDepth > 0)) return;
     if (urlsVisited.contains(url)) {
       return;
     }
     urlsVisited.add(url);
     PageParser.Result result = parserFactory.get(url).parse();
     List<WebCrawlAction> stasks = new ArrayList<>();
     for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
       String key = e.getKey();
       Integer value = e.getValue();
       if (wordCounts.containsKey(key)) wordCounts.put(key, value + wordCounts.get(key));
       else wordCounts.put(key, value);
     }
     for (String url : result.getLinks())
       stasks.add(new WebCrawlAction(url, maxVisitDepth - 1, deadline, wordCounts, urlsVisited, parserFactory, clock, ignoredUrls));
     invokeAll(stasks);
   }

 }

}
