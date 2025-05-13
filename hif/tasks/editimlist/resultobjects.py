import pipeline.infrastructure.basetask as basetask


class EditimlistResult(basetask.Results):
    def __init__(self):
        super(EditimlistResult, self).__init__()
        self.targets = []
        self._max_num_targets = 0
        self.buffer_size_arcsec = 0
        self.img_mode = ''
        self.editmode = 'add'

    def add_target(self, target):
        self.targets.append(target)

    def capture_buffer_size(self, buffsize):
        self.buffer_size_arcsec = buffsize

    def merge_with_context(self, context):
        if not hasattr(context, 'clean_list_pending') or self.editmode == 'replace':
            context.clean_list_pending = []

        for new_target in self.targets:
            context.clean_list_pending.append(new_target)

        # PIPE-592: store img_mode in context
        if not hasattr(context, 'imaging_mode'):
            LOG.warning('imaging_mode property does not exist in context, adding it now.')
        context.imaging_mode = self.img_mode

    @property
    def num_targets(self):
        return len(self.targets)

    @property
    def max_num_targets(self):
        return self._max_num_targets

    def set_max_num_targets(self, max_num_targets):
        self._max_num_targets = max_num_targets

    def __repr__(self):
        return 'Editimlist:'
