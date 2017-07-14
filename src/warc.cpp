
#include <Rcpp.h>
using namespace Rcpp;

#include <stdio.h>
#include <zlib.h>

// [[Rcpp::export]]
List rcpp_hello_world() {

  CharacterVector x = CharacterVector::create( "foo", "bar" )  ;
  NumericVector y   = NumericVector::create( 0.0, 1.0 ) ;
  List z            = List::create( x, y ) ;

  return z ;
}

// [[Rcpp::export]]
CharacterVector rcpp_read_warc(std::string path,
                               std::string filter) {

  FILE *fp = fopen(path.c_str(), "rb");
  if (!fp) Rcpp::stop("Failed to open WARC file.");

  gzFile gzf = gzdopen(fileno(fp), "rb");
  if (!gzf) Rcpp::stop("Failed to open WARC as a compressed file.");

  const int buffer_size = 4 * 1024;
  char buffer[buffer_size] = {'\0'};
  char* line = NULL;

  std::string warc_entry;
  warc_entry.reserve(buffer_size);

  while((line = gzgets(gzf, buffer, buffer_size)) != Z_NULL) {
    warc_entry.append(buffer);
  }

  if (gzf) gzclose(gzf);
  if (fp) fclose(fp);

  CharacterVector result = CharacterVector::create(warc_entry.c_str());
  return result;
}
