/****************************************************************************
 ****************************************************************************/

#include "aws.h"
#include "image.h"

#include "opencv/cvaux.h"
#include "opencv/highgui.h"

#include <fstream>
#include <sstream>
#include <cstdlib>
#include <cassert>
#include <ctime>

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

#define IMAGE_ITEM_ID 			"id"
#define IMAGE_ATTR_NAME			"name"
#define IMAGE_ATTR_SUBJECTID	"sid"
#define IMAGE_ATTR_POSID		"pid"
#define IMAGE_ATTR_FORMAT		"format"
#define IMAGE_ATTR_DIMENSIONS	"dimensions"

#define IMAGE_TABLE_ITEM_ID		"id"
#define IMAGE_TABLE_ATTR_BUCKET		"bucket"
#define IMAGE_TABLE_ATTR_PREFIX 	"prefix"
#define IMAGE_TABLE_ATTR_DOMAIN		"domain"
#define IMAGE_TABLE_ATTR_NEXTID 	"nextid"
#define IMAGE_TABLE_ATTR_EIGENSPACE "eigenspace"

const char *IMAGE_ATTRS[] = {
	IMAGE_ATTR_NAME,
	IMAGE_ATTR_SUBJECTID,
	IMAGE_ATTR_POSID,
	IMAGE_ATTR_FORMAT,
	IMAGE_ATTR_DIMENSIONS,
	NULL
};
const char *IMAGE_TABLE_ATTRS[] = {
	IMAGE_TABLE_ATTR_BUCKET,
	IMAGE_TABLE_ATTR_PREFIX,
	IMAGE_TABLE_ATTR_DOMAIN,
	IMAGE_TABLE_ATTR_NEXTID,
	IMAGE_TABLE_ATTR_EIGENSPACE,
	NULL
};

static const char *ACCESS_KEY_ENV = "AWS_ACCESS_KEY_ID";
static const char *SECRET_ACCESS_KEY_ENV = "AWS_SECRET_ACCESS_KEY";
static const char *IMAGE_CATALOG_SUFFIX = "images";
static const char *EIGEN_PREFIX = "eigen";
static const char *SERIAL_DELIM = " ";

static const char *EVENT_SDB_GET = "sdbget";
static const char *EVENT_SDB_PUT = "sdbput";
static const char *EVENT_S3_GET = "s3get";
static const char *EVENT_S3_PUT = "s3put";
static const char *EVENT_EIGEN_TRAIN = "eigentrain";
static const char *EVENT_EIGEN_LEARN = "eigenlearn";
static const char *EVENT_TOTAL = "total";

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

static char*
get_access_key();

static char*
get_secret_key();

static void
upload_image_eigen(S3ConnectionPtr s3conn, ImageMetadata *meta);

static void
load_image_eigen(S3ConnectionPtr s3conn, ImageMetadata *meta);

static void
upload_image_table_eigenspace(S3ConnectionPtr s3conn, ImageTableMetadata *meta);

static void
load_image_table_eigenspace(S3ConnectionPtr s3conn, ImageTableMetadata *meta);

static void
serial_image_table_meta(ImageTableMetadata *meta, const char* attr, std::string& val);

static void
deserial_image_table_meta(ImageTableMetadata *meta, const char* attr, std::string& val);

static void
serial_image_meta(ImageMetadata *meta, const char* attr, std::string& val);

static void
deserial_image_meta(ImageMetadata *meta, const char* attr, std::string& val);

static IplImage*
load_image(S3ConnectionPtr s3conn, ImageMetadata *meta);

static IplImage*
load_staged_image(S3ConnectionPtr s3conn, const std::string& key);


static void
upload_staged_image(S3ConnectionPtr s3conn, IplImage *image, const std::string& key);


static void
upload_image_table_meta(SDBConnectionPtr sdbconn,
		ImageTableMetadata *meta,
		const char **attrs=NULL);

static void
load_image_table_meta(SDBConnectionPtr sdbconn, ImageTableMetadata *meta);

static void
upload_image_meta(SDBConnectionPtr sdbconn, ImageMetadata *meta, const char **attrs=NULL);

static void
load_image_meta(SDBConnectionPtr sdbconn, ImageMetadata *meta);

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

S3ConnectionPtr
s3connect()
{
	AWSConnectionFactory* factory = AWSConnectionFactory::getInstance();
	S3ConnectionPtr s3conn =  factory->createS3Connection(get_access_key(), get_secret_key());
	return s3conn;
}

SDBConnectionPtr
sdbconnect()
{
	AWSConnectionFactory* factory = AWSConnectionFactory::getInstance();
	SDBConnectionPtr sdbconn = factory->createSDBConnection (get_access_key(), get_secret_key());
	return sdbconn;
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

#define USECS_PER_SEC 1000000LL

static float
timeval_diff(timeval &start, timeval &stop)
{
	timeval diff;
	diff.tv_sec = stop.tv_sec - start.tv_sec ;
	diff.tv_usec = stop.tv_usec - start.tv_usec;

	while (diff.tv_usec < 0) {
		diff.tv_usec += USECS_PER_SEC;
	    diff.tv_sec -= 1;
	}

	float secs = (float)diff.tv_sec + (double)diff.tv_usec/(double)USECS_PER_SEC;
	return secs;
}

const char *Profiler::DELIM = " ";

Profiler::Profiler()
{
	// create a profile filename with a timestamp
	time_t rawtime;
	time (&rawtime);
	struct tm * timeinfo = localtime(&rawtime);
	char buf[128];
	strftime(buf, 128, "%Y-%j_%H-%M-%S.prof", timeinfo);
	filename.assign(buf);
}

Profiler::~Profiler()
{
	flush();
}

void
Profiler::start()
{
	Timer timer;
	timeval t;
	gettimeofday(&t,NULL);
	timer.start = t;
	timers.push_back(timer);
}

void
Profiler::stop(const std::string& val)
{
	assert(timers.size() > 0);
	timeval t;
	gettimeofday(&t,NULL);
	Timer timer = timers.back();
	timer.stop = t;
	float elapsed = timeval_diff(timer.start, timer.stop);
	timers.pop_back();
	events.push_back(std::pair<float, std::string>(elapsed, val));
}

void
Profiler::flush()
{
	char buf[16];
	std::ofstream outs;
	outs.open(filename.c_str(), std::fstream::out | std::fstream::app);
	for (size_t i=0;  i<events.size();  ++i) {
		std::pair<float, std::string> event = events[i];
		sprintf(buf, "%.9f", event.first);
		outs << buf;
		outs << Profiler::DELIM;
		outs << event.second;
		outs << std::endl;
	}
	events.clear();
	outs.close();
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

const char *CVDB::BUCKET = "cloudvision";
const char *CVDB::CATALOG = "cloudvision";


int
CVDB::train(const int table, size_t resolution, std::pair<int, int> range)
{
	profiler.start(); // EVENT_TOTAL
	SDBConnectionPtr sdbconn = sdbconnect();
	ImageTableMetadata *tablemeta = new ImageTableMetadata(table);
	profiler.start();
	load_image_table_meta(sdbconn, tablemeta);
	profiler.stop(EVENT_SDB_GET);

	// load image metadata
	std::vector<ImageMetadata*> metas;
	for (int i=range.first;  i<=range.second;  ++i) {
		ImageMetadata *meta = new ImageMetadata(i);
		meta->imagetable = tablemeta;
		profiler.start();
		load_image_meta(sdbconn, meta);
		profiler.stop(EVENT_SDB_GET);
		metas.push_back(meta);
	}
	assert(metas.size() > 0);

	// load images
	S3ConnectionPtr s3conn = s3connect();
	size_t nimages = metas.size();
	IplImage **images = new IplImage*[nimages];
	char buf[32];
	for (size_t i=0;  i<nimages;  ++i) {
		std::string key(tablemeta->prefix);
		key += "/";
		key += metas[i]->name;
		profiler.start();
		images[i] = load_image(s3conn, metas[i]);
		sprintf(buf, "%d", (images[i]->imageSize)/1000);
		std::string val(EVENT_S3_GET);
		val += Profiler::DELIM;
		val += buf;
		profiler.stop(val);
	}

	// initialize eigenspace
	if (tablemeta->eigenspace != NULL) {
		delete tablemeta->eigenspace;
		tablemeta->eigenspace = NULL;
	}
	sprintf(buf, "%u", nimages);
	std::string val(EVENT_EIGEN_TRAIN);
	val += Profiler::DELIM;
	val += buf;
	profiler.start();
	tablemeta->eigenspace = create_eigen_space(nimages, images, resolution);
	profiler.stop(val);

	// upload eigenspace
	const char *attrs[] = { IMAGE_TABLE_ATTR_EIGENSPACE, NULL };
	profiler.start();
	upload_image_table_meta(sdbconn, tablemeta, attrs);
	profiler.stop(EVENT_SDB_PUT);
	long total_size = 0;
	for (int i=0;  i<tablemeta->eigenspace->dimension;  ++i) {
		total_size += tablemeta->eigenspace->eigenfaces[i]->imageSize;
	}
	sprintf(buf, "%lu", total_size/1000);
	val.assign(EVENT_S3_PUT);
	val += Profiler::DELIM;
	val += buf;
	profiler.start();
	upload_image_table_eigenspace(s3conn, tablemeta);
	profiler.stop(val);

	// clean up
	delete tablemeta;
	for (size_t i=0;  i<nimages;  ++i) {
		cvReleaseImage(&(images[i]));
	}
	delete[] images;
	for (size_t i=0;  i<metas.size();  ++i) {
		delete metas[i];
	}

	profiler.stop(EVENT_TOTAL);
	profiler.flush();

	return EXIT_SUCCESS;
}

int
CVDB::learn(const int table, std::pair<int, int> range)
{
	profiler.start(); // EVENT_TOTAL
	// load table
	SDBConnectionPtr sdbconn = sdbconnect();
	S3ConnectionPtr s3conn = s3connect();
	ImageTableMetadata *tablemeta = new ImageTableMetadata(table);
	profiler.start();
	load_image_table_meta(sdbconn, tablemeta);
	profiler.stop(EVENT_SDB_GET);
	profiler.start();
	load_image_table_eigenspace(s3conn, tablemeta);
	char buf[32];
	long total_size = 0;
	for (int i=0;  i<tablemeta->eigenspace->dimension;  ++i) {
		total_size += tablemeta->eigenspace->eigenfaces[i]->imageSize;
	}
	sprintf(buf, "%lu", total_size/1000);
	std::string val(EVENT_S3_GET);
	val += Profiler::DELIM;
	val += buf;
	profiler.stop(val);

	// to conserve memory, process one image at a time
	for (int i=range.first;  i<=range.second;  ++i) {

		// load image metadata
		ImageMetadata meta(i);
		meta.imagetable = tablemeta;
		profiler.start();
		load_image_meta(sdbconn, &meta);
		profiler.stop(EVENT_SDB_GET);

		// load image
		std::string key(tablemeta->prefix);
		key += "/";
		key += meta.name;
		profiler.start();
		IplImage *image = load_image(s3conn, &meta);
		sprintf(buf, "%d", (image->imageSize)/1000);
		std::string val(EVENT_S3_GET);
		val += Profiler::DELIM;
		val += buf;
		profiler.stop(val);

		// calculate features
		sprintf(buf, "%d", tablemeta->eigenspace->dimension);
		val.assign(EVENT_EIGEN_LEARN);
		val += Profiler::DELIM;
		val += buf;
		meta.features = new float[tablemeta->eigenspace->dimension];
		profiler.start();
		decomposite(tablemeta->eigenspace, image, meta.features);
		profiler.stop(val);

		// upload features
		sprintf(buf, "%d", sizeof(float)*tablemeta->eigenspace->dimension/1000);
		val.assign(EVENT_S3_PUT);
		val += Profiler::DELIM;
		val += buf;
		profiler.start();
		upload_image_eigen(s3conn, &meta);
		profiler.stop(val);

		// clean up
		cvReleaseImage(&image);
	}

	delete tablemeta;
	profiler.stop(EVENT_TOTAL);

	return EXIT_SUCCESS;
}

int
CVDB::query(int tableid, int imageid, std::pair<int, int> range, std::ostream& outs)
{
	profiler.start(); // EVENT_TOTAL

	// load table
	SDBConnectionPtr sdbconn = sdbconnect();
	S3ConnectionPtr s3conn = s3connect();
	ImageTableMetadata *tablemeta = new ImageTableMetadata(tableid);
	load_image_table_meta(sdbconn, tablemeta);

	// load query image
	int vector_size = sizeof(float)*tablemeta->eigenspace->dimension/1000;
	ImageMetadata query_meta(imageid);
	query_meta.imagetable = tablemeta;
	profiler.start();
	load_image_meta(sdbconn, &query_meta);
	profiler.stop(EVENT_SDB_GET);
	char buf[32];
	sprintf(buf, "%d", vector_size);
	std::string val;
	val.assign(EVENT_S3_GET);
	val += Profiler::DELIM;
	val += buf;
	profiler.start();
	load_image_eigen(s3conn, &query_meta);
	profiler.stop(val);

	double min_dist = 0;
	double min_id = 0;

	// to conserve memory, process one image at a time
	for (int i=range.first;  i<=range.second;  ++i) {

		// load image metadata
		ImageMetadata meta(i);
		meta.imagetable = tablemeta;
		profiler.start();
		load_image_meta(sdbconn, &meta);
		profiler.stop(EVENT_SDB_GET);

		// load vector
		profiler.start();
		load_image_eigen(s3conn, &meta);
		profiler.stop(val);

		double dist = vector_distance(tablemeta->eigenspace->dimension,
				query_meta.features, meta.features);
		if (i==range.first || dist < min_dist) {
			min_dist = dist;
			min_id = i;
		}
	}

	// output
	sprintf(buf, "%.6lf", min_dist);
	outs << "{";
	outs << imageid;
	outs << " : ";
	outs << "[ [";
	outs << min_id;
	outs << ", ";
	outs << buf;
	outs << "] ]";
	outs << "}";
	outs << std::endl;

	// clean up
	delete tablemeta;

	profiler.stop(EVENT_TOTAL);

	return EXIT_SUCCESS;
}

int
CVDB::upload(ImageScanner *scanner,
		const int id,
		const std::string& s3prefix)
{
	SDBConnectionPtr sdbconn = sdbconnect();
	// initialize table meta data
	ImageTableMetadata tablemeta(id);
	std::string domain;
	serial_image_table_meta(&tablemeta, IMAGE_TABLE_ITEM_ID, domain);
	std::string imgdomain(domain);
	imgdomain += IMAGE_CATALOG_SUFFIX;
	tablemeta.bucket = CVDB::BUCKET;
	tablemeta.prefix = s3prefix;
	tablemeta.imagedomain = imgdomain;
	tablemeta.nextimageid = 1;

	std::cout << "Creating: " << imgdomain << std::endl;
	CreateDomainResponsePtr res = sdbconn->createDomain(imgdomain);

	// initialize all image meta data
	scanner->open();
	ImageMetadata meta;
	meta.imagetable = &tablemeta;
	while (scanner->next(meta)) {
		meta.id = tablemeta.nextimageid;
		tablemeta.nextimageid++;
		std::cout << "Uploading image: " << meta.id << ", " << meta.name
				<< ", " << meta.subjectid << ", " << meta.poseid << ", "
				<< meta.format << ", " << meta.dimensions.width << ", "
				<< meta.dimensions.height << ", " << meta.dimensions.depth
				<< std::endl;
		upload_image_meta(sdbconn, &meta);
	}
	scanner->close();

	std::cout << "Uploading table: " << tablemeta.id << ", "
			<< tablemeta.bucket << ", " << tablemeta.prefix << ", "
			<< tablemeta.nextimageid << std::endl;
	upload_image_table_meta(sdbconn, &tablemeta);

	return EXIT_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

static IplImage*
load_image(S3ConnectionPtr s3conn, ImageMetadata *meta)
{
	std::string key(meta->imagetable->prefix);
	key += "/" + meta->name;
    GetResponsePtr res = s3conn->get(CVDB::BUCKET, key);
    std::istream& ins = res->getInputStream();
    IplImage *image = read_image(meta, ins);
    return image;
}

static IplImage*
load_staged_image(S3ConnectionPtr s3conn, const std::string& key)
{
    GetResponsePtr res = s3conn->get(CVDB::BUCKET, key);
    std::istream& ins = res->getInputStream();
    int fmt;
	ins >> fmt;
	assert(fmt == PS3M);
	int channels;
	ins >> channels;
	Dimensions dim;
	ins >> dim.width;
	ins >> dim.height;
	ins >> dim.depth;
	ins.ignore();
	IplImage *image = cvCreateImage(cvSize(dim.width,
						dim.height),
						dim.depth,
						channels);
	ins.read(image->imageData, image->imageSize);
	return image;
}

static void
upload_staged_image(S3ConnectionPtr s3conn, IplImage *image, const std::string& key)
{
	assert(image->imageData != NULL);
	std::stringstream ins;
	ins << PS3M << SERIAL_DELIM
		<< image->nChannels << SERIAL_DELIM
		<< image->width << SERIAL_DELIM
		<< image->height << SERIAL_DELIM
		<< image->depth << std::endl;
	ins.write(image->imageData, image->imageSize);
//	int count = ins.gcount();
//	std::cout << count << " " << image->imageData << " " << image->imageSize;
//	assert(count == image->imageSize);
    PutResponsePtr res = s3conn->put(CVDB::BUCKET, key, ins, "binary/octet-stream");
}

static void
upload_image_table_meta(SDBConnectionPtr sdbconn,
		ImageTableMetadata *meta,
			const char **attrs)
{
	if (attrs == NULL) {
		attrs = IMAGE_TABLE_ATTRS;
	}
	const char **attr = attrs;
    std::vector<Attribute> awsattrs;
	while (*attr != NULL) {
		std::string key(*attr);
		std::string value;
		serial_image_table_meta(meta, *attr, value);
  aws::Attribute awsattr(key, value, true);
	    awsattrs.push_back(awsattr);
		attr++;
	}
	std::string value;
	serial_image_table_meta(meta, IMAGE_TABLE_ITEM_ID, value);
	    PutAttributesResponsePtr res = sdbconn->putAttributes(CVDB::CATALOG,
	value, awsattrs);
}

static void
load_image_table_meta(SDBConnectionPtr sdbconn, ImageTableMetadata *meta)
{
	std::string value;
	serial_image_table_meta(meta, IMAGE_TABLE_ITEM_ID, value);
    GetAttributesResponsePtr res = sdbconn->getAttributes(CVDB::CATALOG,
    		value, "");
    res->open();
    AttributePair attr;
    while (res->next(attr)) {
    	deserial_image_table_meta(meta, attr.first.c_str(), attr.second);
    }
    res->close();
}

static void
upload_image_table_eigenspace(S3ConnectionPtr s3conn, ImageTableMetadata *meta)
{
	assert(meta->eigenspace != NULL);
	char buf[32];
	std::string key(meta->prefix);
	key += "/";
	key += EIGEN_PREFIX;
	key +=+ "/average.ps3m";
	upload_staged_image(s3conn, meta->eigenspace->avgface, key);
	for (int i=0;  i<meta->eigenspace->dimension;  ++i) {
		sprintf(buf, "%d.ps3m", i);
		key.assign(meta->prefix);
		key += "/";
		key += EIGEN_PREFIX;
		key += "/";
		key += buf;
		upload_staged_image(s3conn, meta->eigenspace->eigenfaces[i], key);
	}
}

static void
load_image_table_eigenspace(S3ConnectionPtr s3conn, ImageTableMetadata *meta)
{
	assert(meta->eigenspace != NULL);
	char buf[32];
	std::string key(meta->prefix);
	key += "/";
	key += EIGEN_PREFIX;
	key += "/average.ps3m";
	meta->eigenspace->avgface = load_staged_image(s3conn, key);
	meta->eigenspace->eigenfaces = new IplImage*[meta->eigenspace->dimension];
	for (int i=0;  i<meta->eigenspace->dimension;  ++i) {
		sprintf(buf, "%d.ps3m", i);
		key.assign(meta->prefix);
		key += "/";
		key += EIGEN_PREFIX;
		key += "/";
		key += buf;
		meta->eigenspace->eigenfaces[i] = load_staged_image(s3conn, key);
	}
}

static void
upload_image_meta(SDBConnectionPtr sdbconn, ImageMetadata *meta, const char **attrs)
{
	assert(meta->imagetable != NULL);
	if (attrs == NULL) {
		attrs = IMAGE_ATTRS;
	}
	const char **attr = attrs;
    std::vector<Attribute> awsattrs;
	while (*attr != NULL) {
		std::string key(*attr);
		std::string value;
		serial_image_meta(meta, *attr, value);
	    Attribute awsattr(key, value, true);
	    awsattrs.push_back (awsattr);
		attr++;
	}
	std::string item;
	serial_image_meta(meta, IMAGE_ITEM_ID, item);
	    PutAttributesResponsePtr res = sdbconn->putAttributes(meta->imagetable->imagedomain,
	item, awsattrs);
}

static void
load_image_meta(SDBConnectionPtr sdbconn, ImageMetadata *meta)
{
	std::string item;
	serial_image_meta(meta, IMAGE_ITEM_ID, item);
    GetAttributesResponsePtr res = sdbconnect()->getAttributes(meta->imagetable->imagedomain,
    		item, "");
    res->open();
    AttributePair attr;
    while (res->next(attr)) {
    	deserial_image_meta(meta, attr.first.c_str(), attr.second);
    }
    res->close();
}

static void
upload_image_eigen(S3ConnectionPtr s3conn, ImageMetadata *meta)
{
	assert(meta->features != NULL);

	char buf[32];
	std::string key(meta->imagetable->prefix);
	key += "/";
	key += EIGEN_PREFIX;
	key += "/";
	sprintf(buf, "%d.eigen", meta->id);
	key += buf;
	std::stringstream ins;
	ins.write((char*)(meta->features), sizeof(float)*meta->imagetable->eigenspace->dimension);
	PutResponsePtr res = s3conn->put(meta->imagetable->bucket,
			 key, ins, "binary/octet-stream");
}

static void
load_image_eigen(S3ConnectionPtr s3conn, ImageMetadata *meta)
{

	char buf[32];
	std::string key(meta->imagetable->prefix);
	key += "/";
	key += EIGEN_PREFIX;
	key += "/";
	sprintf(buf, "%d.eigen", meta->id);
	key += buf;

	if (meta->features == NULL) {
		meta->features = new float[meta->imagetable->eigenspace->dimension];
	}
    GetResponsePtr res = s3conn->get(CVDB::BUCKET, key);
    std::istream& ins = res->getInputStream();
    ins.read((char*)(meta->features), sizeof(float)*meta->imagetable->eigenspace->dimension);
}

static void
serial_image_table_meta(ImageTableMetadata *meta, const char* attr, std::string& val)
{
	std::stringstream str;

	if (!strcmp(attr, IMAGE_TABLE_ITEM_ID)) {
		str << meta->id;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_BUCKET)) {
		str << meta->bucket;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_PREFIX)) {
		str << meta->prefix;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_DOMAIN)) {
		str << meta->imagedomain;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_NEXTID)) {
		str << meta->nextimageid;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_EIGENSPACE)) {
		if (meta->eigenspace != NULL) {
			str << meta->eigenspace->dimension;
			str << SERIAL_DELIM;
			str << meta->eigenspace->resolution;
		}
	} else {
		std::cout << attr << std::endl;
		assert(0);
	}

	val.assign(str.str());
}

static void
deserial_image_table_meta(ImageTableMetadata *meta, const char* attr, std::string& val)
{
	std::stringstream str(val);

	if (!strcmp(attr, IMAGE_TABLE_ITEM_ID)) {
		str >> meta->id;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_BUCKET)) {
		str >> meta->bucket;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_PREFIX)) {
		str >> meta->prefix;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_DOMAIN)) {
		str >> meta->imagedomain;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_NEXTID)) {
		str >> meta->nextimageid;
	} else if (!strcmp(attr, IMAGE_TABLE_ATTR_EIGENSPACE)) {
		if (val.size() > 0) {
			if (meta->eigenspace == NULL) {
				meta->eigenspace = new Eigenspace;
			}
			str >> meta->eigenspace->dimension;
			str >> meta->eigenspace->resolution;
		}
	} else {
		assert(0);
	}
}

static void
serial_image_meta(ImageMetadata *meta, const char* attr, std::string& val)
{
	std::stringstream str;

	if (!strcmp(attr, IMAGE_ITEM_ID)) {
		str << meta->id;
	} else if (!strcmp(attr, IMAGE_ATTR_NAME)) {
		str << meta->name;
	} else if (!strcmp(attr, IMAGE_ATTR_SUBJECTID)) {
		str << meta->subjectid;
	} else if (!strcmp(attr, IMAGE_ATTR_POSID)) {
		str << meta->poseid;
	} else if (!strcmp(attr, IMAGE_ATTR_FORMAT)) {
		str << meta->format;
	} else if (!strcmp(attr, IMAGE_ATTR_DIMENSIONS)) {
		str << meta->dimensions.width << SERIAL_DELIM;
		str << meta->dimensions.height << SERIAL_DELIM;
		str << meta->dimensions.depth;
	} else {
		assert(0);
	}

	val.assign(str.str());
}

static void
deserial_image_meta(ImageMetadata *meta, const char* attr, std::string& val)
{
	std::stringstream str(val);

	if (!strcmp(attr, IMAGE_ITEM_ID)) {
		str >> meta->id;
	} else if (!strcmp(attr, IMAGE_ATTR_NAME)) {
		str >> meta->name;
	} else if (!strcmp(attr, IMAGE_ATTR_SUBJECTID)) {
		str >> meta->subjectid;
	} else if (!strcmp(attr, IMAGE_ATTR_POSID)) {
		str >> meta->poseid;
	} else if (!strcmp(attr, IMAGE_ATTR_FORMAT)) {
	  str >> meta->format;
	} else if (!strcmp(attr, IMAGE_ATTR_DIMENSIONS)) {
		str >> meta->dimensions.width;
		str >> meta->dimensions.height;
		str >> meta->dimensions.depth;
	} else {
		assert(0);
	}
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

static char*
get_access_key()
{
	char *accesskey = getenv(ACCESS_KEY_ENV);
	return accesskey;
}

static char*
get_secret_key()
{
	char *secretkey = getenv(SECRET_ACCESS_KEY_ENV);
	return secretkey;
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
