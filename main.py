import logging.config

from source.scrapping import Scrapper
from source.emails import Mail
from source.tools import delete_temp, make_loger, get_output_map, read_config
from source.excel import Input, Output


class MainClass:

    def __init__(self):
        self.config = read_config(logger)
        self.output_columns_map = get_output_map(self.config, logger)
        delete_temp(self.config)

    def __bot_process(self):
        while True:
            try:
                message = Mail(self.config)
                message.save_earlier_mail_attachment()
                input = Input(self.config, file_path=message.saved_attachment)
                search_names = input.parse_input(message)
                break
            except Exception as exception:
                delete_temp(self.config)
                if '*Failed_message' not in exception.args:
                    raise exception

        if len(search_names) > 0:
            scrapper = Scrapper(self.config, self.output_columns_map)
            scrapper.run(search_names, message)
            input.add_status_column(scrapper.success_names)
            if not scrapper.forms_df.empty:
                output = Output(self.config)
                output.write(scrapper.forms_df)
                message.success_reply(output_file=output.path, no_processed=input.has_non_processed)
                return
        message.success_reply(output_file=None, no_processed=input.has_non_processed)

    def execution(self):
        try:
            self.__bot_process()
        except Exception as ex:
            if '*Handled_error' not in ex.args:
                try:
                    logger.error(self.config.get('error', 'unexpected'), exc_info=True)
                    unexpected_message = Mail(self.config)
                    unexpected_message.send_fail_to_admin(letter=4)
                except Exception:
                    logger.error(self.config.get('error', 'unexpected'), exc_info=True)
        delete_temp(self.config)


if __name__ == "__main__":
    make_loger()
    logger = logging.getLogger('mainFPDS')
    logger.info('FPDS bot execution started')
    try:
        main = MainClass()
        main.execution()
    except Exception:
        pass
    logger.info('FPDS bot execution ended.')
