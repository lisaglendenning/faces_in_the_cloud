/****************************************************************************
 ****************************************************************************/

#ifndef CLOUDVISION_AWS_H
#define CLOUDVISION_AWS_H

#include "image.h"

#include <opencv/cv.h>
#include <libaws/aws.h>

#include <string>

// for profiling on Linux
#include <sys/time.h>

using namespace aws;


///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

S3ConnectionPtr
s3connect();

SDBConnectionPtr
sdbconnect();

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

typedef struct Timer
{
	timeval start;
	timeval stop;
} Timer;

class Profiler
{
public:
	static const char *DELIM;

	Profiler();
	~Profiler();

	void start();
	void stop(const std::string& val);
	void flush();

private:
	std::string filename;
	std::vector< std::pair<float, std::string> > events;
	std::vector<Timer> timers;
};

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class CVDB
{
public:

	static const char *BUCKET;
	static const char *CATALOG;

	/* Creates an eigenspace for an image table */
	int train(int tableid, size_t resolution, std::pair<int, int> range);

	/* Learns feature vectors for a subset of images */
	int learn(int tableid, std::pair<int, int> range);

	/* Calculates the minimum vector distance for a subset of images */
	int query(int tableid, int imageid, std::pair<int, int> range, std::ostream& outs);

	/* Extracts and uploads image database metadata in bulk */
	int upload(ImageScanner *scanner, int tableid, const std::string& s3prefix);

private:
	Profiler profiler;

};

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

#endif // CLOUDVISION_AWS_H
