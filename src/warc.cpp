
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

  CharacterVector result = CharacterVector::create("");

  FILE *fp = fopen(path.c_str(), "rb");
  if (!fp) Rcpp::stop("Failed to open WARC file.");

  gzFile gzf = gzdopen(fileno(fp), "rb");
  if (!gzf) Rcpp::stop("Failed to open WARC as a compressed file.");

  if (gzf) gzclose(fp);
  if (fp) fclose(fp);

  return result;
}
