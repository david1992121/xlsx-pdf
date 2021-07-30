import os, time, logging
import win32com.client
from pywintypes import com_error
from logging.handlers import TimedRotatingFileHandler

_DELAY = 0.05  # seconds
_TIMEOUT = 60.0  # seconds

def _com_call_wrapper(f, *args, **kwargs):
    """
    COMWrapper support function. 
    Repeats calls when 'Call was rejected by callee.' exception occurs.
    """
    # Unwrap inputs
    args = [arg._wrapped_object if isinstance(arg, ComWrapper) else arg for arg in args]
    kwargs = dict([(key, value._wrapped_object)
                   if isinstance(value, ComWrapper)
                   else (key, value)
                   for key, value in dict(kwargs).items()])

    start_time = None
    while True:
        try:
            result = f(*args, **kwargs)
        except com_error as e:
            if e.strerror == 'Call was rejected by callee.' or e.strerror == "呼び出し先が呼び出しを拒否しました。":
                if start_time is None:
                    start_time = time.time()
                    logging.warning('Call was rejected by callee.')

                elif time.time() - start_time >= _TIMEOUT:
                    raise

                time.sleep(_DELAY)
                continue

            raise

        break

    if isinstance(result, win32com.client.CDispatch) or callable(result):
        return ComWrapper(result)
    return result


class ComWrapper(object):
    """
    Class to wrap COM objects to repeat calls when 'Call was rejected by callee.' exception occurs.
    """

    def __init__(self, wrapped_object):
        assert isinstance(wrapped_object, win32com.client.CDispatch) or callable(wrapped_object)
        self.__dict__['_wrapped_object'] = wrapped_object

    def __getattr__(self, item):
        return _com_call_wrapper(self._wrapped_object.__getattr__, item)

    def __getitem__(self, item):
        return _com_call_wrapper(self._wrapped_object.__getitem__, item)

    def __setattr__(self, key, value):
        _com_call_wrapper(self._wrapped_object.__setattr__, key, value)

    def __setitem__(self, key, value):
        _com_call_wrapper(self._wrapped_object.__setitem__, key, value)

    def __call__(self, *args, **kwargs):
        return _com_call_wrapper(self._wrapped_object.__call__, *args, **kwargs)

    def __repr__(self):
        return 'ComWrapper<{}>'.format(repr(self._wrapped_object))

class PDFChanger():
    def __init__(self, queue, error_queue, index, visible = False):
        self.queue = queue
        self.error_queue = error_queue
        self.init_log(index)
        self.excel = ComWrapper(win32com.client.DispatchEx("Excel.Application"))
        self.excel.Visible = visible

    def init_log(self, index):
        logpath = os.path.join(os.path.dirname(__file__), 'logs')
        if not os.path.exists(logpath):
            os.makedirs(logpath)
        follower_path = os.path.join(logpath, 'changer')
        if not os.path.exists(follower_path):
            os.mkdir(follower_path)

        logname = "log_{}".format(index)

        # set rotate log files
        file_handler = TimedRotatingFileHandler(os.path.join(follower_path, logname), when = 'H', interval=1, backupCount=7, encoding='utf8')
        file_handler.suffix = "%Y%m%d%H00"
        logging.basicConfig(
            level=logging.INFO,
            format='フォロワープ %(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=(file_handler,),
        )

    def execute_changing(self):        
        while True:
            logging.info("now get queue data")
            queue_data = self.queue.get()
            logging.info(queue_data)
            input_file = queue_data["input"]
            output_file = queue_data["output"]

            if input_file != "quit":

                try:
                    logging.info("対象ファイルパス: {0}".format(input_file))

                    # Open
                    self.wb = self.excel.Workbooks.Open(input_file)

                    # Specify the sheet you want to save by index. 1 is the first (leftmost) sheet.
                    ws_index_list = [1,2,3]
                    self.wb.WorkSheets(ws_index_list).Select()

                    # Save
                    logging.info("出力のファイルパス: {0}".format(output_file))
                    self.wb.ActiveSheet.ExportAsFixedFormat(0, output_file)

                    self.wb.Close()
                    self.wb = None

                    # Remove that file
                    os.remove(input_file)

                except Exception as e:
                    logging.info("{0}のファイルをPDF化中にエラー発生".format(input_file))
                    logging.error(e)

                    base_name = os.path.basename(input_file)
                    file_name = os.path.splitext(base_name)[0]
                    logging.info("エラーファイルリストに追加")
                    self.error_queue.put(file_name)
                finally:
                    logging.info("{0}のファイルPDF処理終了".format(output_file))
                    self.queue.task_done()

            else:
                self.queue.task_done()
                break
    
    def __del__(self):
        logging.info("エクセル終了")
        self.excel.Quit()
