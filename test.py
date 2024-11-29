import os
import logging

os.environ["OPENPYPE_DATABASE_NAME"] = "openpype"
os.environ["OPENPYPE_ROOT"] = "C:/Users/max.pareschi/Documents/DEV/OpenPype"

from openpype.modules.ttd_addon.lib.new_editorial import (
    SequenceData,
    ImageData,
    get_image_data,
    get_sequence_data,
    resample_sequence
)


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(name)s:%(levelname)s >>> %(message)s",
    )
    logger = logging.getLogger("Testing")

    image_data_check_paths = [
        "C:/Users/max.pareschi/Desktop/WAR_0028_plateMain_v0000_main.0985.exr",
        "C:/Users/max.pareschi/Desktop/test.exr",
        "C:/Users/max.pareschi/Desktop/409_104_0007_lyot_DOG_v006.0001.mov",
        "C:/Users/max.pareschi/Desktop/DMR402_001_030_v014.mov",
    ]
    
    file_list = [
        "DMR401_060_010_plateMain_v0000_source.0001001.exr",
        "DMR401_060_010_plateMain_v0000_source.0001002.exr",
        "DMR401_060_010_plateMain_v0000_source.0001003.exr",
        "DMR401_060_010_plateMain_v0000_source.0001004.exr",
        "DMR401_060_010_plateMain_v0000_source.0001005.exr",
        "DMR401_060_010_plateMain_v0000_source.0001006.exr",
        "DMR401_060_010_plateMain_v0000_source.0001007.exr",
        "DMR401_060_010_plateMain_v0000_source.0001008.exr",
        "DMR401_060_010_plateMain_v0000_source.0001009.exr",
        "DMR401_060_010_plateMain_v0000_source.0001010.exr",
        "DMR401_060_010_plateMain_v0000_source.0001011.exr",
        "DMR401_060_010_plateMain_v0000_source.0001012.exr",
        "DMR401_060_010_plateMain_v0000_source.0001013.exr",
        "DMR401_060_010_plateMain_v0000_source.0001014.exr",
        "DMR401_060_010_plateMain_v0000_source.0001015.exr",
        "DMR401_060_010_plateMain_v0000_source.0001016.exr",
        "DMR401_060_010_plateMain_v0000_source.0001017.exr",
        "DMR401_060_010_plateMain_v0000_source.0001018.exr",
        "DMR401_060_010_plateMain_v0000_source.0001019.exr",
        "DMR401_060_010_plateMain_v0000_source.0001020.exr",
        "DMR401_060_010_plateMain_v0000_source.0001021.exr",
        "DMR401_060_010_plateMain_v0000_source.0001022.exr",
        "DMR401_060_010_plateMain_v0000_source.0001023.exr",
        "DMR401_060_010_plateMain_v0000_source.0001024.exr",
        "DMR401_060_010_plateMain_v0000_source.0001025.exr",
        "DMR401_060_010_plateMain_v0000_source.0001026.exr",
        "DMR401_060_010_plateMain_v0000_source.0001027.exr",
        "DMR401_060_010_plateMain_v0000_source.0001028.exr",
        "DMR401_060_010_plateMain_v0000_source.0001029.exr",
        "DMR401_060_010_plateMain_v0000_source.0001030.exr",
        "DMR401_060_010_plateMain_v0000_source.0001031.exr",
        "DMR401_060_010_plateMain_v0000_source.0001032.exr",
        "DMR401_060_010_plateMain_v0000_source.0001033.exr",
        "DMR401_060_010_plateMain_v0000_source.0001034.exr",
        "DMR401_060_010_plateMain_v0000_source.0001035.exr",
        "DMR401_060_010_plateMain_v0000_source.0001036.exr",
        "DMR401_060_010_plateMain_v0000_source.0001037.exr",
        "DMR401_060_010_plateMain_v0000_source.0001038.exr",
        "DMR401_060_010_plateMain_v0000_source.0001039.exr",
        "DMR401_060_010_plateMain_v0000_source.0001040.exr",
        "DMR401_060_010_plateMain_v0000_source.0001041.exr",
        "DMR401_060_010_plateMain_v0000_source.0001042.exr",
        "DMR401_060_010_plateMain_v0000_source.0001043.exr",
        "DMR401_060_010_plateMain_v0000_source.0001044.exr",
        "DMR401_060_010_plateMain_v0000_source.0001045.exr",
        "DMR401_060_010_plateMain_v0000_source.0001046.exr",
        "DMR401_060_010_plateMain_v0000_source.0001047.exr",
        "DMR401_060_010_plateMain_v0000_source.0001048.exr",
        "DMR401_060_010_plateMain_v0000_source.0001049.exr",
        "DMR401_060_010_plateMain_v0000_source.0001050.exr",
        "DMR401_060_010_plateMain_v0000_source.0001051.exr",
        "DMR401_060_010_plateMain_v0000_source.0001052.exr",
        "DMR401_060_010_plateMain_v0000_source.0001053.exr",
        "DMR401_060_010_plateMain_v0000_source.0001054.exr",
        "DMR401_060_010_plateMain_v0000_source.0001055.exr",
        "DMR401_060_010_plateMain_v0000_source.0001056.exr",
        "DMR401_060_010_plateMain_v0000_source.0001057.exr",
        "DMR401_060_010_plateMain_v0000_source.0001058.exr",
        "DMR401_060_010_plateMain_v0000_source.0001059.exr",
        "DMR401_060_010_plateMain_v0000_source.0001060.exr",
        "DMR401_060_010_plateMain_v0000_source.0001061.exr",
        "DMR401_060_010_plateMain_v0000_source.0001062.exr",
        "DMR401_060_010_plateMain_v0000_source.0001063.exr",
        "DMR401_060_010_plateMain_v0000_source.0001064.exr",
        "DMR401_060_010_plateMain_v0000_source.0001065.exr",
        "DMR401_060_010_plateMain_v0000_source.0001066.exr",
        "DMR401_060_010_plateMain_v0000_source.0001067.exr",
        "DMR401_060_010_plateMain_v0000_source.0001068.exr",
        "DMR401_060_010_plateMain_v0000_source.0001069.exr",
        "DMR401_060_010_plateMain_v0000_source.0001070.exr",
        "DMR401_060_010_plateMain_v0000_source.0001071.exr",
        "DMR401_060_010_plateMain_v0000_source.0001072.exr",
        "DMR401_060_010_plateMain_v0000_source.0001073.exr",
        "DMR401_060_010_plateMain_v0000_source.0001074.exr",
        "DMR401_060_010_plateMain_v0000_source.0001075.exr",
        "DMR401_060_010_plateMain_v0000_source.0001076.exr",
        "DMR401_060_010_plateMain_v0000_source.0001077.exr",
        "DMR401_060_010_plateMain_v0000_source.0001078.exr",
        "DMR401_060_010_plateMain_v0000_source.0001079.exr",
        "DMR401_060_010_plateMain_v0000_source.0001080.exr",
        "DMR401_060_010_plateMain_v0000_source.0001081.exr",
        "DMR401_060_010_plateMain_v0000_source.0001082.exr",
        "DMR401_060_010_plateMain_v0000_source.0001083.exr",
        "DMR401_060_010_plateMain_v0000_source.0001084.exr",
        "DMR401_060_010_plateMain_v0000_source.0001085.exr",
        "DMR401_060_010_plateMain_v0000_source.0001086.exr",
        "DMR401_060_010_plateMain_v0000_source.0001087.exr",
        "DMR401_060_010_plateMain_v0000_source.0001088.exr",
        "DMR401_060_010_plateMain_v0000_source.0001089.exr",
        "DMR401_060_010_plateMain_v0000_source.0001090.exr",
        "DMR401_060_010_plateMain_v0000_source.0001091.exr",
        "DMR401_060_010_plateMain_v0000_source.0001092.exr",
        "DMR401_060_010_plateMain_v0000_source.0001093.exr",
        "DMR401_060_010_plateMain_v0000_source.0001094.exr",
        "DMR401_060_010_plateMain_v0000_source.0001095.exr",
        "DMR401_060_010_plateMain_v0000_source.0001096.exr",
        "DMR401_060_010_plateMain_v0000_source.0001097.exr",
        "DMR401_060_010_plateMain_v0000_source.0001098.exr",
        "DMR401_060_010_plateMain_v0000_source.0001099.exr"
    ]
    root_path = "D:/TESTING_ROOT/projects/TEST_SOLOMON/shots/060/DMR401_060_010/publish/plate/plateMain/v0000"
    file_list_rooted = [os.path.join(root_path, file).replace("\\", "/") for file in file_list]

    logger.info("_________________________\n")

    # for image_path in image_data_check_paths:
    #     image_data = get_sequence_data(file_list=[image_path])
    #     logger.info(image_data)
    #     logger.info("_________________________\n")
    # 
    # sequences = get_sequence_data(root_path = root_path)
    # logger.info(sequences)
    # 
    # logger.info("_________________________\n")
    # 
    # sequences = get_sequence_data(file_list = file_list, root_path = root_path)
    # logger.info(sequences)
    #
    # logger.info("_________________________\n")

    sq = get_sequence_data(file_list = file_list_rooted)
    # logger.info(sq)

    logger.info("_________________________\n")

    import copy

    sqnew: SequenceData = copy.deepcopy(sq) #type: ignore
    sqnew.head = "DEVASTO_001_0010."
    sqnew.tail = ".jpg"
    sqnew.frame_start = 1
    sqnew.padding = 10

    newsq = resample_sequence(sqnew, suffix="proxy")

    logger.info(newsq)



