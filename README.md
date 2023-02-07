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
1. ##### Decision Level Fusion of Two Stream
Two streams of information characterize this methodology. The CNN will be utilized to classify the wildlife species, outputting a softmax layer containing each species probability within the image. 
A separate Support Vector Machine (SVG) will be trained on spatio-temporal information in order to classify each specie, outputting a softmax output containing the same features as the CNN.
The combination of these two decision level feature vectors will be conducted through statistical mean still to be decided.
2. ##### Feature Concatenation
3. ##### Layer Concatenation
Utilizing existing CNN architectures, a concatenation layer will be placed before the softmax output layer. 
This concatenation layer will serve to fuse higher levels of semantic information between what was detected within the image, 
and the provided spatio-temporal information. Utilisation of pre-concatenation layers will be experimented upon.[1]
4. ##### Model Fusion

## Results
Still to be completed

## External Links
- **Thesis Document** (inactive)
- [iNaturalist](https://www.inaturalist.org/)

### Scientific Papers
[1] K. Tang, M. Paluri, L. Fei-Fei, R. Fergus, and L. Bourdev, “Improving Image Classification with Location Context,” 2015 IEEE International Conference on Computer Vision (ICCV), Dec. 2015, doi: 10.1109/iccv.2015.121.‌