import constants
import ismrmrd
import ctypes
import os
from datetime import datetime
import h5py
import random
import threading

import logging
import socket
import numpy as np
import uuid

class Connection:
    def __init__(self, socket, savedata, savedataFile = "", savedataFolder = "", savedataGroup = "dataset"):
        self.savedata       = savedata
        self.savedataFile   = savedataFile
        self.savedataFolder = savedataFolder
        self.savedataGroup  = savedataGroup
        self.mrdFilePath    = None
        self.dset           = None
        self.socket         = socket
        self.is_exhausted   = False
        self.sentAcqs       = 0
        self.sentImages     = 0
        self.sentWaveforms  = 0
        self.recvAcqs       = 0
        self.recvImages     = 0
        self.recvWaveforms  = 0
        self.lock           = threading.Lock()
        self.queue          = []
        self.handlers       = {
            constants.MRD_MESSAGE_CONFIG_FILE:         self.read_config_file,
            constants.MRD_MESSAGE_CONFIG_TEXT:         self.read_config_text,
            constants.MRD_MESSAGE_METADATA_XML_TEXT:   self.read_metadata,
            constants.MRD_MESSAGE_CLOSE:               self.read_close,
            constants.MRD_MESSAGE_TEXT:                self.read_text,
            constants.MRD_MESSAGE_ISMRMRD_ACQUISITION: self.read_acquisition,
            constants.MRD_MESSAGE_ISMRMRD_WAVEFORM:    self.read_waveform,
            constants.MRD_MESSAGE_ISMRMRD_IMAGE:       self.read_image,
            constants.MRD_MESSAGE_ISMRMRD_FEEDBACK:    self.read_feedback,
            constants.MRD_MESSAGE_ISMRMRD_STORE_DATA_QUERY:       self.read_store_data_query,
            constants.MRD_MESSAGE_ISMRMRD_STORE_DATA_RESPONSE:    self.read_store_data_response,
            constants.MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_QUERY:    self.read_retrieve_data_query,
            constants.MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_RESPONSE: self.read_retrieve_data_response
        }

    def create_save_file(self):
        if self.savedata is True:
            # Create savedata folder, if necessary
            if ((self.savedataFolder) and (not os.path.exists(self.savedataFolder))):
                os.makedirs(self.savedataFolder)
                logging.debug("Created folder " + self.savedataFolder + " to save incoming data")

            if (self.savedataFile):
                self.mrdFilePath = self.savedataFile
            else:
                self.mrdFilePath = os.path.join(self.savedataFolder, "MRD_input_" + datetime.now().strftime("%Y-%m-%d-%H%M%S" + "_" + str(random.randint(0,100)) + ".h5"))

            # Create HDF5 file to store incoming MRD data
            logging.info("Incoming data will be saved to: '%s' in group '%s'", self.mrdFilePath, self.savedataGroup)
            self.dset = ismrmrd.Dataset(self.mrdFilePath, self.savedataGroup)
            self.dset._file.require_group(self.savedataGroup)

    def save_additional_config(self, configAdditionalText):
        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()

            self.dset._file.require_group("dataset")
            dsetConfigAdditional = self.dset._dataset.require_dataset('configAdditional',shape=(1,), dtype=h5py.special_dtype(vlen=bytes))
            dsetConfigAdditional[0] = bytes(configAdditionalText, 'utf-8')

    def send_logging(self, level, contents):
        try:
            formatted_contents = "%s %s" % (level, contents)
        except:
            logging.warning("Unsupported logging level: " + level)
            formatted_contents = contents

        self.send_text(formatted_contents)

    def __iter__(self):
        while not (self.is_exhausted and (len(self.queue) == 0)):
            yield self.next()

    def __next__(self):
        return self.next()

    def read(self, nbytes):
        return self.socket.recv(nbytes, socket.MSG_WAITALL)

    def peek(self, nbytes):
        return self.socket.recv(nbytes, socket.MSG_PEEK)

    def next(self):
        # Get next parsed message from either queue or socket, ignoring data storage responses

        # Check queue first
        for i,item in enumerate(self.queue):
            if not isinstance(item, (constants.MrdStoreDataResponse, constants.MrdRetrieveDataResponse)):
                return self.queue.pop(i)

        # Check the socket
        with self.lock:
            while True:
                id = self.read_mrd_message_identifier()

                if id is None:
                    return None

                handler = self.handlers.get(id, lambda: Connection.unknown_message_identifier(id))
                item = handler()
                if isinstance(item, (constants.MrdStoreDataResponse, constants.MrdRetrieveDataResponse)):
                    logging.debug("connection.next() is adding a data response to queue!")
                    self.queue.append(item)
                else:
                    return item

    def get_data_response(self, responseType, uid):
        # Check queue first
        for i,item in enumerate(self.queue):
            if isinstance(item, responseType):
                if item.uid == uid:
                    logging.info("Found a response for UID: %s (%d in self queue)", uid, i)
                    return self.queue.pop(i)

        # Look through the next few messages on the socket for the response with matching UID
        maxPeekMessages = 10
        with self.lock:
            for i in range(maxPeekMessages):
                id = self.read_mrd_message_identifier()

                if id is None:
                    return None

                handler = self.handlers.get(id, lambda: Connection.unknown_message_identifier(id))
                item = handler()

                if isinstance(item, responseType) and item.uid == uid:
                    logging.info("Found a response for UID: %s (%d in network queue)", uid, i)
                    return item
                else:
                    logging.debug("get_data_response is adding a non-data-response to queue!")
                    self.queue.append(item)
        return None

    def shutdown_close(self):
        # Encapsulate shutdown in a try block because the socket may have
        # already been closed on the other side
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except:
            pass
        self.socket.close()
        logging.info("Socket closed")

    @staticmethod
    def unknown_message_identifier(identifier):
        logging.error("Received unknown message type: %d", identifier)
        raise StopIteration

    def read_mrd_message_identifier(self):
        try:
            identifier_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_IDENTIFIER)
        except ConnectionResetError:
            logging.error("Connection closed unexpectedly")
            self.is_exhausted = True
            return

        if (len(identifier_bytes) == 0):
            self.is_exhausted = True
            return

        return constants.MrdMessageIdentifier.unpack(identifier_bytes)[0]

    def peek_mrd_message_identifier(self):
        try:
            identifier_bytes = self.peek(constants.SIZEOF_MRD_MESSAGE_IDENTIFIER)
        except ConnectionResetError:
            logging.error("Connection closed unexpectedly")
            self.is_exhausted = True
            return

        if (len(identifier_bytes) == 0):
            self.is_exhausted = True
            return

        return constants.MrdMessageIdentifier.unpack(identifier_bytes)[0]

    def read_mrd_message_length(self):
        length_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_LENGTH)
        return constants.MrdMessageLength.unpack(length_bytes)[0]

    def read_mrd_message_attrib_length(self):
        length_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_ATTRIB_LENGTH)
        return constants.MrdMessageAttribLength.unpack(length_bytes)[0]

    # ----- MRD_MESSAGE_CONFIG_FILE (1) ----------------------------------------
    # This message contains the file name of a configuration file used for 
    # image reconstruction/post-processing.  The file must exist on the server.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Config file name (1024 bytes, char          )
    def send_config_file(self, filename):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_CONFIG_FILE (1)")
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_CONFIG_FILE))
            self.socket.send(constants.MrdMessageConfigurationFile.pack(filename.encode()))

    def read_config_file(self):
        logging.info("<-- Received MRD_MESSAGE_CONFIG_FILE (1)")
        config_file_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_CONFIGURATION_FILE)
        config_file = constants.MrdMessageConfigurationFile.unpack(config_file_bytes)[0].decode("utf-8")
        config_file = config_file.split('\x00',1)[0]  # Strip off null terminators in fixed 1024 size

        logging.debug("    " + config_file)
        if (config_file == "savedataonly"):
            logging.info("Save data, but no processing based on config")
            if self.savedata is True:
                logging.debug("Saving data is already enabled")
            else:
                self.savedata = True
                self.create_save_file()

        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()

            self.dset._file.require_group("dataset")
            dsetConfigFile = self.dset._dataset.require_dataset('config_file',shape=(1,), dtype=h5py.special_dtype(vlen=bytes))
            dsetConfigFile[0] = bytes(config_file, 'utf-8')

        return config_file

    # ----- MRD_MESSAGE_CONFIG_TEXT (2) --------------------------------------
    # This message contains the configuration information (text contents) used 
    # for image reconstruction/post-processing.  Text is null-terminated.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Length           (   4 bytes, uint32_t      )
    #   Config text data (  variable, char          )
    def send_config_text(self, contents):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_CONFIG_TEXT (2)")
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_CONFIG_TEXT))
            contents_with_nul = '%s\0' % contents # Add null terminator
            self.socket.send(constants.MrdMessageLength.pack(len(contents_with_nul.encode())))
            self.socket.send(contents_with_nul.encode())

    def read_config_text(self):
        logging.info("<-- Received MRD_MESSAGE_CONFIG_TEXT (2)")
        length = self.read_mrd_message_length()
        config = self.read(length)
        config = config.decode("utf-8").split('\x00',1)[0]  # Strip off null teminator

        logging.debug("    " + config)

        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()

            self.dset._file.require_group("dataset")
            dsetConfig = self.dset._dataset.require_dataset('config',shape=(1,), dtype=h5py.special_dtype(vlen=bytes))
            dsetConfig[0] = bytes(config, 'utf-8')

        return config

    # ----- MRD_MESSAGE_METADATA_XML_TEXT (3) -----------------------------------
    # This message contains the metadata for the entire dataset, formatted as
    # MRD XML flexible data header text.  Text is null-terminated.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Length           (   4 bytes, uint32_t      )
    #   Text xml data    (  variable, char          )
    def send_metadata(self, contents):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_METADATA_XML_TEXT (3)")
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_METADATA_XML_TEXT))
            contents_with_nul = '%s\0' % contents # Add null terminator
            self.socket.send(constants.MrdMessageLength.pack(len(contents_with_nul.encode())))
            self.socket.send(contents_with_nul.encode())

    def read_metadata(self):
        logging.info("<-- Received MRD_MESSAGE_METADATA_XML_TEXT (3)")
        length = self.read_mrd_message_length()
        metadata = self.read(length)
        metadata = metadata.decode("utf-8").split('\x00',1)[0]  # Strip off null teminator

        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()

            logging.debug("    Saving XML header to file")
            self.dset.write_xml_header(bytes(metadata, 'utf-8'))

        return metadata

    # ----- MRD_MESSAGE_CLOSE (4) ----------------------------------------------
    # This message signals that all data has been sent (either from server or client).
    def send_close(self):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_CLOSE (4)")
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_CLOSE))

    def read_close(self):
        logging.info("<-- Received MRD_MESSAGE_CLOSE (4)")

        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()

            logging.debug("Closing file %s", self.dset._file.filename)
            self.dset.close()
            self.dset = None

        self.is_exhausted = True
        return

    # ----- MRD_MESSAGE_TEXT (5) -----------------------------------
    # This message contains arbitrary text data.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Length           (   4 bytes, uint32_t      )
    #   Text data        (  variable, char          )
    def send_text(self, contents):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_TEXT (5)")
            logging.info("    %s", contents)
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_TEXT))
            contents_with_nul = '%s\0' % contents # Add null terminator
            self.socket.send(constants.MrdMessageLength.pack(len(contents_with_nul.encode())))
            self.socket.send(contents_with_nul.encode())

    def read_text(self):
        logging.info("<-- Received MRD_MESSAGE_TEXT (5)")
        length = self.read_mrd_message_length()
        text = self.read(length)
        text = text.decode("utf-8").split('\x00',1)[0]  # Strip off null teminator
        logging.info("    %s", text)
        return text

    # ----- MRD_MESSAGE_ISMRMRD_ACQUISITION (1008) -----------------------------
    # This message contains raw k-space data from a single readout.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Fixed header     ( 340 bytes, mixed         )
    #   Trajectory       (  variable, float         )
    #   Raw k-space data (  variable, float         )
    def send_acquisition(self, acquisition):
        with self.lock:
            self.sentAcqs += 1
            if (self.sentAcqs == 1) or (self.sentAcqs % 100 == 0):
                logging.info("--> Sending MRD_MESSAGE_ISMRMRD_ACQUISITION (1008) (total: %d)", self.sentAcqs)

            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_ACQUISITION))
            acquisition.serialize_into(self.socket.send)

    def read_acquisition(self):
        self.recvAcqs += 1
        if (self.recvAcqs == 1) or (self.recvAcqs % 100 == 0):
            logging.info("<-- Received MRD_MESSAGE_ISMRMRD_ACQUISITION (1008) (total: %d)", self.recvAcqs)

        acq = ismrmrd.Acquisition.deserialize_from(self.read)

        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()

            self.dset.append_acquisition(acq)

        return acq

    # ----- MRD_MESSAGE_ISMRMRD_IMAGE (1022) -----------------------------------
    # This message contains a single [x y z cha] image.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Fixed header     ( 198 bytes, mixed         )
    #   Attribute length (   8 bytes, uint64_t      )
    #   Attribute data   (  variable, char          )
    #   Image data       (  variable, variable      )
    def send_image(self, images):
        with self.lock:
            if not isinstance(images, list):
                images = [images]

            logging.info("--> Sending MRD_MESSAGE_ISMRMRD_IMAGE (1022) (%d images)", len(images))
            for image in images:
                if image is None:
                    continue

                self.sentImages += 1
                self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_IMAGE))
                image.serialize_into(self.socket.send)

            # Explicit version of serialize_into() for more verbose debugging
            # self.socket.send(image.getHead())
            # self.socket.send(constants.MrdMessageAttribLength.pack(len(image.attribute_string)))
            # self.socket.send(bytes(image.attribute_string, 'utf-8'))
            # self.socket.send(bytes(image.data))

    def read_image(self):
        self.recvImages += 1
        logging.info("<-- Received MRD_MESSAGE_ISMRMRD_IMAGE (1022)")
        # return ismrmrd.Image.deserialize_from(self.read)

        # Explicit version of deserialize_from() for more verbose debugging
        logging.debug("   Reading in %d bytes of image header", ctypes.sizeof(ismrmrd.ImageHeader))
        header_bytes = self.read(ctypes.sizeof(ismrmrd.ImageHeader))

        attribute_length_bytes = self.read(ctypes.sizeof(ctypes.c_uint64))
        attribute_length = ctypes.c_uint64.from_buffer_copy(attribute_length_bytes)
        logging.debug("   Reading in %d bytes of attributes", attribute_length.value)

        attribute_bytes = self.read(attribute_length.value)
        if (attribute_length.value > 25000):
            logging.debug("   Attributes (truncated): %s", attribute_bytes[0:24999].decode('utf-8'))
        else:
            logging.debug("   Attributes: %s", attribute_bytes.decode('utf-8'))

        image = ismrmrd.Image(header_bytes, attribute_bytes.decode('utf-8').split('\x00',1)[0])  # Strip off null teminator

        logging.info("    Image is size %d x %d x %d with %d channels of type %s", image.getHead().matrix_size[0], image.getHead().matrix_size[1], image.getHead().matrix_size[2], image.channels, ismrmrd.get_dtype_from_data_type(image.data_type))
        def calculate_number_of_entries(nchannels, xs, ys, zs):
            return nchannels * xs * ys * zs

        nentries = calculate_number_of_entries(image.channels, *image.getHead().matrix_size)
        nbytes = nentries * ismrmrd.get_dtype_from_data_type(image.data_type).itemsize

        logging.debug("Reading in %d bytes of image data", nbytes)
        data_bytes = self.read(nbytes)

        image.data.ravel()[:] = np.frombuffer(data_bytes, dtype=ismrmrd.get_dtype_from_data_type(image.data_type))

        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()
            self.dset.append_image("image_%d" % image.image_series_index, image)

        return image

    # ----- MRD_MESSAGE_ISMRMRD_WAVEFORM (1026) -----------------------------
    # This message contains abitrary (e.g. physio) waveform data.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Fixed header     ( 240 bytes, mixed         )
    #   Waveform data    (  variable, uint32_t      )
    def send_waveform(self, waveform):
        with self.lock:
            self.sentWaveforms += 1
            if (self.sentWaveforms == 1) or (self.sentWaveforms % 100 == 0):
                logging.info("--> Sending MRD_MESSAGE_ISMRMRD_WAVEFORM (1026) (total: %d)", self.sentWaveforms)

            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_WAVEFORM))
            waveform.serialize_into(self.socket.send)

    def read_waveform(self):
        self.recvWaveforms += 1
        if (self.recvWaveforms == 1) or (self.recvWaveforms % 100 == 0):
            logging.info("<-- Received MRD_MESSAGE_ISMRMRD_WAVEFORM (1026) (total: %d)", self.recvWaveforms)

        waveform = ismrmrd.Waveform.deserialize_from(self.read)

        if self.savedata is True:
            if self.dset is None:
                self.create_save_file()

            self.dset.append_waveform(waveform)

        return waveform

    # ----- MRD_MESSAGE_ISMRMRD_FEEDBACK (1028) -----------------------------
    # This message contains real-time feedback data.
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Name length      (   4 bytes, uint32_t      )
    #   Name             (  variable, char          )
    #   Data length      (   4 bytes, uint32_t      )
    #   Data             (  variable, char          )
    def send_feedback(self, name, data):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_FEEDBACK (1028)")
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_FEEDBACK))

            name_with_nul = '%s\0' % name # Add null terminator
            self.socket.send(constants.MrdMessageLength.pack(len(name_with_nul.encode())))
            self.socket.send(name_with_nul.encode())

            self.socket.send(constants.MrdMessageLength.pack(ctypes.sizeof(data)))
            self.socket.send(data)

    def read_feedback(self):
        logging.info("<-- Received MRD_MESSAGE_FEEDBACK (1028)")
        nameLength = self.read_mrd_message_length()
        name = self.read(nameLength)
        name = name.decode("utf-8").split('\x00',1)[0]  # Strip off null teminator

        dataLength = self.read_mrd_message_length()
        data = self.read(dataLength)

        logging.info("    Name is %s with %d bytes of data: %s" % (name, dataLength, np.frombuffer(data, dtype=np.uint8)))

        class FeedbackData(ctypes.Structure):
            _pack_ = 1
            _fields_ = [
                ('myBool',   ctypes.c_bool),      #          1 byte
                ('myInt64s', ctypes.c_int64 *2),  # 2 * 8 = 16 bytes
                ('myFloat',  ctypes.c_float)      #          4 bytes
            ]                                     #       = 21 bytes total

        dataStruct = FeedbackData()
        ctypes.memmove(ctypes.addressof(dataStruct), data, ctypes.sizeof(dataStruct))
        logging.info("Received feedback struct with data:")
        logging.info("dataStruct.myBool    is: %s" % dataStruct.myBool)
        logging.info("dataStruct.myInt64s are: %s" % list(dataStruct.myInt64s))
        logging.info("dataStruct.myFloats  is: %s" % dataStruct.myFloat)

        return (name, data)

    # ----- MRD_MESSAGE_ISMRMRD_STORE_DATA_QUERY (1030) -----------------------------
    # This message contains abitrary data to be stored by the client
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Unique ID        (  16 bytes, 128-bit label )
    #   Name length      (   4 bytes, uint32_t      )
    #   Name             (  variable, char          )
    #   Data length      (   4 bytes, uint32_t      )
    #   Data             (  variable, char          )
    def send_store_data_query(self, storeDataQuery: constants.MrdStoreDataQuery):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_ISMRMRD_STORE_DATA_QUERY (1030)")
            logging.info("    UID: %s, Name: %s, Data: %d bytes", storeDataQuery.uid, storeDataQuery.name, len(storeDataQuery.data))
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_STORE_DATA_QUERY))

            self.socket.send(storeDataQuery.uid.bytes)

            name_with_nul = '%s\0' % storeDataQuery.name # Add null terminator
            self.socket.send(constants.MrdMessageLength.pack(len(name_with_nul.encode())))
            self.socket.send(name_with_nul.encode())

            self.socket.send(constants.MrdMessageLength.pack(len(storeDataQuery.data)))
            self.socket.send(storeDataQuery.data)

    def read_store_data_query(self) -> constants.MrdStoreDataQuery:
        logging.info("<-- Received MRD_MESSAGE_ISMRMRD_STORE_DATA_QUERY (1030)")

        uid_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_UUID)
        uid = uuid.UUID(bytes=uid_bytes)

        nameLength = self.read_mrd_message_length()
        name = self.read(nameLength)
        name = name.decode("utf-8").split('\x00',1)[0]  # Strip off null teminator

        dataLength = self.read_mrd_message_length()
        data = self.read(dataLength)

        storeDataQuery = constants.MrdStoreDataQuery(name, data, uid)
        logging.info("    UID: %s, Name: %s, Data: %d bytes", storeDataQuery.uid, storeDataQuery.name, len(storeDataQuery.data))

        return storeDataQuery

    # ----- MRD_MESSAGE_ISMRMRD_STORE_DATA_RESPONSE (1031) -----------------------------
    # This message contains a status response to MRD_MESSAGE_ISMRMRD_STORE_DATA_RESPONSE
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Unique ID        (  16 bytes, 128-bit label )
    #   Status           (   4 bytes, uint32_t      )
    def send_store_data_response(self, storeDataResponse: constants.MrdStoreDataResponse):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_ISMRMRD_STORE_DATA_RESPONSE (1031)")
            logging.info("    UID: %s, Status: %d", storeDataResponse.uid, storeDataResponse.status)
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_STORE_DATA_RESPONSE))

            self.socket.send(storeDataResponse.uid.bytes)

            self.socket.send(constants.MrdMessageStatus.pack(storeDataResponse.status))

    def read_store_data_response(self) -> constants.MrdStoreDataResponse:
        logging.info("<-- Received MRD_MESSAGE_ISMRMRD_STORE_DATA_RESPONSE (1031)")

        uid_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_UUID)
        uid = uuid.UUID(bytes=uid_bytes)

        status_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_STATUS)
        status = constants.MrdMessageStatus.unpack(status_bytes)[0]

        storeDataResponse = constants.MrdStoreDataResponse(status, uid)
        logging.info("    UID: %s, Status: %d", storeDataResponse.uid, storeDataResponse.status)
        return storeDataResponse

    def store_data(self, name, data):
        storeDataQuery = constants.MrdStoreDataQuery(name, data, uuid.uuid4())

        self.send_store_data_query(storeDataQuery)
        res = self.get_data_response(constants.MrdStoreDataResponse, storeDataQuery.uid)

        return res.status

    # ----- MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_QUERY (1032) -----------------------------
    # This message contains abitrary data to be retrieved from the client
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Unique ID        (  16 bytes, 128-bit label )
    #   Name length      (   4 bytes, uint32_t      )
    #   Name             (  variable, char          )
    def send_retrieve_data_query(self, retrieveDataQuery: constants.MrdRetrieveDataQuery):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_QUERY (1032)")
            logging.info("    UID: %s, Name: %s", retrieveDataQuery.uid, retrieveDataQuery.name)
            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_QUERY))

            self.socket.send(retrieveDataQuery.uid.bytes)

            name_with_nul = '%s\0' % retrieveDataQuery.name # Add null terminator
            self.socket.send(constants.MrdMessageLength.pack(len(name_with_nul.encode())))
            self.socket.send(name_with_nul.encode())

    def read_retrieve_data_query(self) -> constants.MrdRetrieveDataQuery:
        logging.info("<-- Received MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_QUERY (1032)")

        uid_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_UUID)
        uid = uuid.UUID(bytes=uid_bytes)

        nameLength = self.read_mrd_message_length()
        name = self.read(nameLength)
        name = name.decode("utf-8").split('\x00',1)[0]  # Strip off null teminator

        retrieveDataQuery = constants.MrdRetrieveDataQuery(name, uid)
        logging.info("    UID: %s, Name: %s", retrieveDataQuery.uid, retrieveDataQuery.name)

        return retrieveDataQuery

    # ----- MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_RESPONSE (1033) -----------------------------
    # This message contains a status response to MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_QUERY
    # Message consists of:
    #   ID               (   2 bytes, unsigned short)
    #   Unique ID        (  16 bytes, 128-bit label )
    #   Status           (   4 bytes, uint32_t      )
    #   Name length      (   4 bytes, uint32_t      )
    #   Name             (  variable, char          )
    #   Data length      (   4 bytes, uint32_t      )
    #   Data             (  variable, char          )
    def send_retrieve_data_response(self, retrieveDataResponse: constants.MrdRetrieveDataResponse):
        with self.lock:
            logging.info("--> Sending MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_RESPONSE (1033)")
            logging.info("    UID: %s, Status: %d, Name: %s, Data: %d bytes", retrieveDataResponse.uid, retrieveDataResponse.status, retrieveDataResponse.name, len(retrieveDataResponse.data))

            self.socket.send(constants.MrdMessageIdentifier.pack(constants.MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_RESPONSE))

            self.socket.send(retrieveDataResponse.uid.bytes)

            self.socket.send(constants.MrdMessageStatus.pack(retrieveDataResponse.status))

            name_with_nul = '%s\0' % retrieveDataResponse.name # Add null terminator
            self.socket.send(constants.MrdMessageLength.pack(len(name_with_nul.encode())))
            self.socket.send(name_with_nul.encode())

            self.socket.send(constants.MrdMessageLength.pack(len(retrieveDataResponse.data)))
            self.socket.send(retrieveDataResponse.data)

    def read_retrieve_data_response(self) -> constants.MrdRetrieveDataResponse:
        logging.info("<-- Received MRD_MESSAGE_ISMRMRD_RETRIEVE_DATA_RESPONSE (1033)")

        uid_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_UUID)
        uid = uuid.UUID(bytes=uid_bytes)

        status_bytes = self.read(constants.SIZEOF_MRD_MESSAGE_STATUS)
        status = constants.MrdMessageStatus.unpack(status_bytes)[0]

        nameLength = self.read_mrd_message_length()
        name = self.read(nameLength)
        name = name.decode("utf-8").split('\x00',1)[0]  # Strip off null teminator

        dataLength = self.read_mrd_message_length()
        data = self.read(dataLength)

        retrieveDataResponse = constants.MrdRetrieveDataResponse(status, name, data, uid)
        logging.info("    UID: %s, Status: %d, Name: %s, Data: %d bytes", retrieveDataResponse.uid, retrieveDataResponse.status, retrieveDataResponse.name, len(retrieveDataResponse.data))

        return retrieveDataResponse

    def retrieve_data(self, name):
        retrieveDataQuery = constants.MrdRetrieveDataQuery(name, uuid.uuid4())

        self.send_retrieve_data_query(retrieveDataQuery)
        res = self.get_data_response(constants.MrdRetrieveDataResponse, retrieveDataQuery.uid)

        return res
