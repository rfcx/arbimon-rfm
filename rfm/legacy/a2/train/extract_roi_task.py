import os.path

import numpy

import a2.job.tasks
import a2.runtime as runtime
import a2.audio.recording


@runtime.tags.tag('task_type', 'train.surface.extract_samples')
class ExtractRoiTask(a2.job.tasks.Task):
    """Task that extracts a roi from a given recording and stores it in efs.
        Inputs:[
            recording_id,
            [species_id, songtype_id],
            [x1, x2, y1, y2]
        ]
        Output:
            efs://~/{:species_id}_{:songtype_id}/rois/{:step_id}.npz
                - the extracted image, and its metadata
    """
    def run(self):
        rec_id, (species_id, songtype_id), bbox = self.get_args()
        recording = a2.audio.recording.Recording(rec_id)
        roi = recording.get_spectrogram(bbox)
        
        output_path = self.get_workspace_path("{}_{}/rois".format(
            species_id, 
            songtype_id
        ))
        
        numpy.savez(
            os.path.join(output_path, "{}.npz".format(self.taskId)),
            sample_rate=recording.sample_rate,
            roi=roi,
            bbox=bbox,
        )
        