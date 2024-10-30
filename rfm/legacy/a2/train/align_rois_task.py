import os
import os.path

import numpy
import png

import a2.job.tasks
import a2.runtime as runtime
import a2.runtime.tmp
import a2.audio.roiset


@runtime.tags.tag('task_type', 'train.surface.align_create')
class ExtractRoiTask(a2.job.tasks.Task):
    """Task that aligns all rois read from the working folder.
        Inputs:[
            species_id, 
            songtype_id
        ]
            efs://~/{:species_id}_{:songtype_id}/rois/*.npz

        Output:
            efs://~/{:species_id}_{:songtype_id}/surface.npz
    """
    def run(self):
        species_id, songtype_id = self.get_args()
        roi_class = "{}_{}".format(species_id, songtype_id)
        
        base_path = self.get_workspace_path(roi_class)
        rois_path = os.path.join(base_path, 'rois')        
        
        roi_set = self.create_roiset(rois_path, roi_class)
        roi_set.alignSamples()
        
        self.upload_image(roi_class, roi_set)

        numpy.savez(
            os.path.join(base_path, "surface.npz"),
            roi=roi_set.getSurface(),
            sample_rate=roi_set.setSampleRate,
            fbounds=(roi_set.lowestFreq, roi_set.highestFreq),
            max_cols=roi_set.maxColumns
        )

    def create_roiset(self, rois_path, roi_class):
        """Creates a roiset from the *.npz roi files in the given folder, for the given class"""
        roi_set = None
        for roi_file in (x for x in os.listdir(rois_path) if x[-4:] == '.npz'):
            with numpy.load(os.path.join(rois_path, roi_file)) as roidata:
                roi_spec, bbox, sample_rate = roidata['roi'], roidata['bbox'], roidata['sample_rate']
                
                if not roi_set:
                    roi_set = a2.audio.roiset.Roiset(roi_class, float(sample_rate))

                roi_set.addRoi(
                    float(bbox[2]), float(bbox[3]), float(sample_rate),
                    roi_spec,
                    roi_spec.shape[0], roi_spec.shape[1]
                )

        if not roi_set:
            raise StandardError('Cannot create pattern surface from rois, no training data found.')

        return roi_set

    def upload_image(self, roi_class, roi_set):
        "uploads the computed surface for the given class to the bucket."
        key = "project_{}/models/job_{}_{}.png".format(
            self.get_project_id(),
            self.get_job_id(),
            roi_class
        )
        
        surface = roi_set.getSurface()
        
        specToShow = numpy.zeros(shape=(0, int(surface.shape[1])))

        # ummm, remove -10,000 values?
        surface[surface == -10000] = float('nan')
        # hmmm, copied from Rafa, but... what??
        for j in range(surface.shape[0]):
            if abs(sum(surface[j, :])) > 0.0:
                specToShow = numpy.vstack((specToShow, surface[j, :]))
        # fill in with the min
        specToShow[specToShow[:, :] == 0] = numpy.min(numpy.min(specToShow))
        # compute values range
        smin = min([min((specToShow[j])) for j in range(specToShow.shape[0])])
        smax = max([max((specToShow[j])) for j in range(specToShow.shape[0])])
        # compute image (scaled to 0-255 and inverted in range)
        x = 255*(1-((specToShow - smin)/(smax-smin)))
        # create monochromatic png image with 8bpp
        image = png.from_array(x, 'L;8')

        with a2.runtime.tmp.tmpfile(suffix=".png") as tmpfile:
            image.save(tmpfile.file)
            tmpfile.close_file()
            runtime.bucket.upload_filename(key, tmpfile.filename, 'public-read')

