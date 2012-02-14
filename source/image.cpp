/****************************************************************************
 ****************************************************************************/

#include "image.h"
#include "opencv/cvaux.h"


Eigenspace::Eigenspace()
 : resolution(0), dimension(0), eigenfaces(NULL), avgface(NULL) { }

Eigenspace::~Eigenspace()
{
	if (eigenfaces != NULL) {
		for (int i=0;  i<dimension;  ++i) {
			cvReleaseImage(&(eigenfaces[i]));
		}
		delete[] eigenfaces;
	}
	if (avgface != NULL) {
		cvReleaseImage(&avgface);
	}
}

ImageTableMetadata::ImageTableMetadata(const int id)
  : id(id), eigenspace(NULL) { }

ImageTableMetadata::~ImageTableMetadata()
{
	if (eigenspace != NULL) {
		delete eigenspace;
	}
}

ImageMetadata::ImageMetadata(const int id)
 : id(id), features(NULL), imagetable(NULL)
{
}

ImageMetadata::~ImageMetadata()
{
	if (features != NULL) {
		delete[] features;
	}
}

ImageScanner::ImageScanner() { }


Eigenspace *
create_eigen_space(size_t nimages, IplImage* images[], size_t resolution)
{
	assert(images != NULL);

	// resize images
	IplImage **input_images = new IplImage*[nimages];
	for (size_t i=0;  i<nimages;  ++i) {
		input_images[i] = cvCreateImage(cvSize(resolution, resolution),
				images[i]->depth, images[i]->nChannels);
		cvResize(images[i], input_images[i]);
	}

	// initialize space
	Eigenspace *eigenspace = new Eigenspace;
	eigenspace->avgface = cvCreateImage(cvSize(resolution, resolution),
			IPL_DEPTH_32F, 1);
	eigenspace->resolution = resolution;
	eigenspace->dimension = nimages - 1;
	eigenspace->eigenfaces = new IplImage*[eigenspace->dimension];
	for (int i=0;  i<eigenspace->dimension;  ++i) {
		eigenspace->eigenfaces[i] = cvCreateImage(cvSize(resolution, resolution),
									  IPL_DEPTH_32F,
									  1);
	}

	// calculate
	CvTermCriteria termCond = cvTermCriteria(CV_TERMCRIT_ITER,
			eigenspace->dimension, 1);
	cvCalcEigenObjects(nimages,
			input_images,
			eigenspace->eigenfaces,
			0, 0, 0,
			&termCond, 
			eigenspace->avgface, NULL);

	// clean up
	for (size_t i=0;  i<nimages;  ++i) {
		cvReleaseImage(&(input_images[i]));
	}
	delete[] input_images;

	return eigenspace;
}

void
decomposite(Eigenspace *eigenspace, IplImage *image, float features[])
{
	assert(eigenspace != NULL);
	assert(image != NULL);

	// resize image
	IplImage *input_image = cvCreateImage(cvSize(eigenspace->resolution,
												 eigenspace->resolution),
				image->depth, image->nChannels);
		cvResize(image, input_image);

    cvEigenDecomposite(input_image,
            eigenspace->dimension,
            eigenspace->eigenfaces,
            0, 0,
            eigenspace->avgface,
            features);
    cvReleaseImage(&input_image);
}

double
vector_distance(const size_t dimension, float * const a, float * const b)
{
	double distSq = 0;
	for (size_t i=0;  i<dimension;  ++i) {
		float d_i =	a[i] -	b[i];
		distSq += d_i*d_i;
	}

	return distSq;
}

IplImage*
read_image(ImageMetadata *meta, std::istream& ins)
{
	if (meta->format == PGM) {
		Dimensions dim;
		int fmt;
		read_header(ins, dim, fmt);

		assert(dim.width == meta->dimensions.width);
		assert(dim.height == meta->dimensions.height);
		assert(dim.depth == meta->dimensions.depth);
		assert(fmt == meta->format);
		IplImage *image = cvCreateImage(cvSize(meta->dimensions.width,
							meta->dimensions.height),
							meta->dimensions.depth,
							1);
		ins.read(image->imageData, image->imageSize);
		int count = ins.gcount();
		assert(count == image->imageSize);
		return image;
	} else {
		assert(0);
	}
	return NULL;
}

void
read_header(std::istream& ins, Dimensions& dimensions, int& format)
{
	// header
	std::string type;
	ins >> type;
	if (!strcmp(type.c_str(), "P5")) {
		format = PGM;
	} else {
		assert(0);
	}
	ins >> dimensions.width;
	ins >> dimensions.height;
	int maxval;
	ins >> maxval;
	if (maxval < 256) {
		dimensions.depth = IPL_DEPTH_8U;
	} else {
		assert(0);
	}
	ins.ignore();
}
