import requests
from postgres_manager import DbManager
from loguru import logger
from utils import timeit


class VinDecoder(DbManager):
	def __init__(self):
		self._url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/'
		self._vin_list = self._enrich_vin_list()
		self._counter = 0
		self._vin_dict_list = []

	def _enrich_vin_list(self):
		query_response = (self.select_query('select vin from carvana_cars limit'))
		query_response = [item[0] for item in query_response]
		return query_response

	@timeit
	def process_vin(self):
		logger.debug('Start proccessing vin codes')
		vin_batch_list = [self._vin_list[i:i + 50] for i in range(0, len(self._vin_list), 50)]
		counter = 0
		try:
			for vin_batch in (vin_batch_list):
				post_fields = {'format': 'json', 'data': ';'.join(vin_batch)}
				cars = self._get_cars_from_request(post_fields)
				for car in cars:
					if 'Model' in car:
						vin_dict = {
						'vin' : car['VIN'],
						'brand' : car['Make'],
						'model' : car['Model']
						}
						self._counter += 1
						self._vin_dict_list.append(vin_dict)
				counter += 50
				if counter == 1000:
					if self._vin_dict_list:
						logger.debug('Start writing data to DB')
						self.write_list_of_dicts_to_db(self._vin_dict_list,'vin_model_match')
						self._vin_dict_list = []
						counter = 0
			if self._vin_dict_list:
					logger.debug('Start writing data to DB')
					self.write_list_of_dicts_to_db(self._vin_dict_list,'vin_model_match')
					self._vin_dict_list = []
			logger.success(f'Successfully processed {self._counter} vin codes')
			return True
		finally:
				if self._vin_dict_list:
						logger.debug('Start writing data to DB')
						self.write_list_of_dicts_to_db(self._vin_dict_list,'vin_model_match')
						self._vin_dict_list = []

	def _get_cars_from_request(self,post_fields):
		for _ in range(10):
			response = requests.post(self._url, data=post_fields)
			if response.status_code == 200:
				cars = response.json()['Results']
				return cars
			else:
				logger.error("Can't get response, retrying")
				continue
		logger.critical("Can't get response")
		return False

