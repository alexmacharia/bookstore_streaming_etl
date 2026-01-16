class Config():
    def __init__(self):
        self.base_data_dir = 's3://bookstore-storage-001'
        self.base_checkpoint_dir = 's3://bookstore-checkpoints-002'
        self.db_name = 'bookstore_db'
        self.maxFilesPerTrigger = 1000