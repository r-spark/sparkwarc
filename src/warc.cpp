#include <Rcpp.h>
using namespace Rcpp;

#include <stdio.h>
#include <zlib.h>

std::size_t rcpp_find_tag(std::string const &line, std::size_t pos) {
  auto const tag_start = line.find("<", pos);
  if (tag_start != std::string::npos &&
      line.find(">", tag_start + 1) != std::string::npos) {
    return tag_start;
  }

  return std::string::npos;
}

constexpr std::size_t kBufSz = 4 * 1024;
constexpr std::size_t kAvgWarcSz = 40 * 1024;
std::string const kWarcSep = "WARC/1.0";

// [[Rcpp::export]]
DataFrame rcpp_read_warc(std::string const &path, std::string const &filter,
                         std::string const &include) {

  FILE *fp = fopen(path.c_str(), "rb");
  if (!fp)
    Rcpp::stop("Failed to open WARC file.");

  gzFile gzf = gzdopen(fileno(fp), "rb");
  if (!gzf)
    Rcpp::stop("Failed to open WARC as a compressed file.");

  char buf[kBufSz] = {'\0'};

  std::list<std::string> warc_entries;

  std::string warc_entry;
  warc_entry.reserve(kAvgWarcSz);

  bool one_matched = false;

  long stats_tags_total = 0;
  std::list<long> warc_stats;

  while (gzgets(gzf, buf, kBufSz) != Z_NULL) {
    std::string line(buf);

    if (!filter.empty() && !one_matched) {
      one_matched = line.find(filter) != std::string::npos;
    }

    if (line.substr(0, kWarcSep.size()) == kWarcSep && warc_entry.size() > 0) {
      if (filter.empty() || one_matched) {
        warc_entries.emplace_back(std::move(warc_entry));
        warc_stats.push_back(stats_tags_total);
        stats_tags_total = 0;
      }

      one_matched = false;

      warc_entry.clear();
    }

    auto tag_start = rcpp_find_tag(line, 0);
    while (tag_start != std::string::npos) {
      stats_tags_total += 1;
      tag_start = rcpp_find_tag(line, tag_start + 1);
    }

    if (include.empty() || line.find(include) != std::string::npos) {
      warc_entry.append(std::move(line));
    }
  }

  if (gzf)
    gzclose(gzf);
  if (fp)
    fclose(fp);

  std::size_t idxEntry = 0;
  CharacterVector results(warc_entries.size());
  std::for_each(std::make_move_iterator(warc_entries.begin()),
                std::make_move_iterator(warc_entries.end()),
                [&results, &idxEntry](std::string &&entry) {
                  results[idxEntry++] = std::move(entry);
                });

  std::size_t idxStat = 0;
  NumericVector stats(warc_stats.size());
  std::for_each(warc_stats.begin(), warc_stats.end(),
                [&stats, &idxStat](long &stat) { stats[idxStat++] = stat; });

  return DataFrame::create(Named("tags") = std::move(stats),
                           _["content"] = std::move(results));
}
