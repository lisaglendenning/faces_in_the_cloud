/****************************************************************************
 ****************************************************************************/

#ifndef CLOUDVISION_IMAGE_H
#define CLOUDVISION_IMAGE_H


#include <string>
#include <vector>

#include "opencv/cv.h"

using namespace cv;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

enum { PGM, PS3M };

typedef struct Dimensions
{
	int width;
	int height;
	int depth;
} Dimensions;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

typedef struct Eigenspace
{
	Eigenspace();
	~Eigenspace();

	size_t resolution;
	int dimension;
	IplImage **eigenfaces;
	IplImage *avgface;
} Eigenspace;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

typedef struct ImageTableMetadata
{
	ImageTableMetadata(int id);
	~ImageTableMetadata();

  int id;;
	std::string bucket;
    std::string prefix;
    std::string imagedomain;
	int nextimageid;
	Eigenspace *eigenspace;
private:
	ImageTableMetadata();

} ImageTableMetadata;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

typedef struct ImageMetadata
{
	ImageMetadata(int id=0);
	~ImageMetadata();

	int id;
    std::string name;
	int subjectid;
	int poseid;
	int format;
	Dimensions dimensions;
	float *features;
	ImageTableMetadata *imagetable;
} ImageMetadata;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class ImageScanner
{
public:
	virtual void open() = 0;
	virtual bool next(ImageMetadata&) = 0;
	virtual void close() = 0;
protected:
	ImageScanner();
};

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

IplImage*
read_image(ImageMetadata *meta, std::istream& ins);

void
read_header(std::istream& ins, Dimensions& dimensions, int& format);

Eigenspace *
create_eigen_space(size_t nimages, IplImage* images[], size_t resolution);

void
decomposite(Eigenspace *eigenspace, IplImage *image, float features[]);

double
vector_distance(size_t dimension, float *a,  float *b);

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

#endif // CLOUDVISION_IMAGE_H
