# spatiotemporal_classification
Bachelor's thesis investigating global wildlife classification using spatio-temporal contextual information.

## Summary
Automated wildlife classification is essential within ecological studies, wildlife conservation and management, specifically fulfilling the roles of species population estimates, individual identification, and behavioural patterns.

Current classification methods make use of deep Convolutional Neural Networks (CNN) in order to accurately classify wildlife species.
Wildlife sightings are prone to various forms of imbalance and noise, ranging from tourism bias (sighting time and species), through to weather and orientation requiring additional information to support CNN classification.

In summary, AlexNet or VGG-16 CNN architectures will be utilized with pre-trained models in order to reduce training time. 
Multiple methods of contextual information inclusion are considered including:
1. Decision-level fusion of two streams
2. Feature concatenation
3. Layer concatenation
4. Model-level fusion

More information on the proposed methods can be found within the [Methods](#methods) section and [Thesis document](#external-links).

A proposed use-case utilizes tourist's public social media postings in order to support wildlife park's population estimates and tracking. 
Two considerations are kept in mind. Firstly, this information must be kept confidential in order to eliminate the threat of granting poachers additional knowledge. 
Secondly, the immediacy of social media posting and the staggering quantity of historical resources make it the largest potential source of wildlife historical and current data.

## Training Data
The training data is obtained from [iNaturalist](https://www.inaturalist.org/), a citizen-science based platform tasked with generating global research-grade, annotated flora and fauna images to facilitate computer vision developement. 
This thesis focuses exclusively on a subgroup of mammalian species, of which examples are below followed by the dataset characteristics:

| ![](https://inaturalist-open-data.s3.amazonaws.com/photos/254323960/large.jpeg) | ![](https://inaturalist-open-data.s3.amazonaws.com/photos/254318111/large.jpeg) |
|--------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| ![](https://inaturalist-open-data.s3.amazonaws.com/photos/254306053/large.jpg) | ![](https://static.inaturalist.org/photos/254074172/large.jpg)                                                                           |


### Data Characteristics
Still to be determined

## Methods
Still to be completed

## Results
Still to be completed

## External Links
- **Thesis Document** (inactive)
- [iNaturalist](https://www.inaturalist.org/)

### Scientific Papers