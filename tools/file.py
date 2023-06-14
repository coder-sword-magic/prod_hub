def zip_folder(folder_path:str, 
               output_path:str) -> bool:
    """
    压缩文件夹
    folder path: 源文件夹
    output_path: 目标压缩文件路径
    """
    import zipfile
    import os
    try:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip_obj:
            for foldername, _, filenames in os.walk(folder_path):
                for filename in filenames:
                    file_path = os.path.join(foldername, filename)
                    zip_obj.write(file_path, os.path.relpath(
                        file_path, folder_path))
        return True
    except:
        return False
