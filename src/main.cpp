#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <boost/asio.hpp>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <mutex> // Adicionado para std::mutex

using boost::asio::ip::tcp;

#pragma pack(push, 1)
struct LogRecord
{
    char sensor_id[32];    // supondo um ID de sensor de até 32 caracteres
    std::time_t timestamp; // timestamp UNIX
    double value;          // valor da leitura
};
#pragma pack(pop)

class Session : public std::enable_shared_from_this<Session>
{
public:
    Session(tcp::socket socket, std::unordered_map<std::string, std::ofstream> &logs, std::mutex &logs_mutex)
        : socket_(std::move(socket)), logs_(logs), logs_mutex_(logs_mutex) {}

    void start()
    {
        read_message();
    }

private:
    void read_message()
    {
        auto self(shared_from_this());
        boost::asio::async_read_until(socket_, buffer_, "\r\n",
                                      [this, self](boost::system::error_code ec, std::size_t length)
                                      {
                                          if (!ec)
                                          {
                                              std::istream is(&buffer_);
                                              std::string message;
                                              std::getline(is, message);
                                              process_message(message);
                                              read_message();
                                          }
                                      });
    }

    void process_message(const std::string &message)
    {
        if (message.rfind("LOG|", 0) == 0)
        {
            auto parts = split_message(message);
            if (parts.size() == 4)
            {
                const std::string &sensor_id = parts[1];
                const std::string &timestamp_str = parts[2]; // Renomeado para evitar conflito
                const std::string &value_str = parts[3];     // Renomeado para evitar conflito

                LogRecord record;
                std::strncpy(record.sensor_id, sensor_id.c_str(), sizeof(record.sensor_id) - 1);
                record.sensor_id[sizeof(record.sensor_id) - 1] = '\\0'; // Garantir terminação nula
                record.timestamp = string_to_time_t(timestamp_str);
                try
                {
                    record.value = std::stod(value_str);

                    std::lock_guard<std::mutex> lock(logs_mutex_);
                    if (logs_.find(sensor_id) == logs_.end())
                    {
                        logs_[sensor_id].open(sensor_id + ".log", std::ios::binary | std::ios::app);
                    }

                    if (logs_[sensor_id].is_open())
                    {
                        logs_[sensor_id].write(reinterpret_cast<const char *>(&record), sizeof(record));
                        logs_[sensor_id].flush(); // Garantir que os dados sejam escritos imediatamente
                    }
                    else
                    {
                        // Opcional: Logar erro se o arquivo não pôde ser aberto
                        std::cerr << "Error: Could not open log file for sensor " << sensor_id << std::endl;
                    }
                }
                catch (const std::invalid_argument &ia)
                {
                    std::cerr << "Invalid argument: " << ia.what() << " for sensor_id: " << sensor_id << " value: " << value_str << std::endl;
                    // Opcional: Enviar erro de volta ao cliente
                }
                catch (const std::out_of_range &oor)
                {
                    std::cerr << "Out of Range error: " << oor.what() << " for sensor_id: " << sensor_id << " value: " << value_str << std::endl;
                    // Opcional: Enviar erro de volta ao cliente
                }
            }
        }
        else if (message.rfind("GET|", 0) == 0)
        {
            auto parts = split_message(message);
            if (parts.size() == 3)
            {
                const std::string &sensor_id = parts[1];
                int num_records = 0;
                try
                {
                    num_records = std::stoi(parts[2]);
                }
                catch (const std::invalid_argument &ia)
                {
                    std::cerr << "Invalid argument for num_records: " << ia.what() << " value: " << parts[2] << std::endl;
                    std::string error = "ERROR|INVALID_NUM_RECORDS\\r\\n";
                    boost::asio::write(socket_, boost::asio::buffer(error));
                    return;
                }
                catch (const std::out_of_range &oor)
                {
                    std::cerr << "Out of Range error for num_records: " << oor.what() << " value: " << parts[2] << std::endl;
                    std::string error = "ERROR|INVALID_NUM_RECORDS\\r\\n";
                    boost::asio::write(socket_, boost::asio::buffer(error));
                    return;
                }

                bool sensor_exists;
                {
                    std::lock_guard<std::mutex> lock(logs_mutex_);
                    sensor_exists = (logs_.count(sensor_id) > 0);
                }

                if (sensor_exists)
                {
                    std::ifstream log_file(sensor_id + ".log", std::ios::binary);
                    if (!log_file.is_open())
                    {
                        // Se o arquivo não puder ser aberto, mesmo que o sensor exista no mapa (improvável se o log foi escrito)
                        std::string error = "ERROR|CANNOT_READ_LOG_FILE\\r\\n";
                        boost::asio::write(socket_, boost::asio::buffer(error));
                        return;
                    }

                    log_file.seekg(0, std::ios::end);
                    std::streampos file_size = log_file.tellg();
                    int total_records = file_size / sizeof(LogRecord);

                    num_records = std::min(num_records, total_records);
                    log_file.seekg(-num_records * sizeof(LogRecord), std::ios::end);

                    std::ostringstream response;
                    response << num_records;

                    for (int i = 0; i < num_records; ++i)
                    {
                        LogRecord record;
                        log_file.read(reinterpret_cast<char *>(&record), sizeof(record));
                        response << ";" << time_t_to_string(record.timestamp) << "|" << record.value;
                    }

                    response << "\r\n";
                    boost::asio::write(socket_, boost::asio::buffer(response.str()));
                }
                else
                {
                    std::string error = "ERROR|INVALID_SENSOR_ID\r\n";
                    boost::asio::write(socket_, boost::asio::buffer(error));
                }
            }
        }
    }

    std::vector<std::string> split_message(const std::string &message)
    {
        std::vector<std::string> parts;
        std::istringstream stream(message);
        std::string part;
        while (std::getline(stream, part, '|'))
        {
            parts.push_back(part);
        }
        return parts;
    }

    std::time_t string_to_time_t(const std::string &time_string)
    {
        std::tm tm = {};
        std::istringstream ss(time_string);
        // Adicionar verificação de falha para get_time
        if (!(ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S")))
        {
            // Lançar uma exceção ou retornar um valor indicando erro
            // Por simplicidade, vamos logar e retornar um time_t inválido (0 ou -1)
            // Em um cenário real, um tratamento de erro mais robusto seria necessário.
            std::cerr << "Error parsing time string: " << time_string << std::endl;
            return static_cast<std::time_t>(-1); // Ou lançar std::runtime_error
        }
        // Verificar se a conversão foi completa e não há caracteres restantes inválidos
        if (ss.fail() || !ss.eof())
        {
            // Se houver caracteres restantes ou a conversão falhou de outra forma
            char c;
            if (ss.clear(), ss >> c)
            { // Tenta ler o próximo caractere
                std::cerr << "Error parsing time string: " << time_string << " - unexpected character: " << c << std::endl;
            }
            else
            {
                std::cerr << "Error parsing time string: " << time_string << " - format error." << std::endl;
            }
            return static_cast<std::time_t>(-1);
        }
        return std::mktime(&tm);
    }

    std::string time_t_to_string(std::time_t time)
    {
        std::tm *tm = std::localtime(&time);
        std::ostringstream ss;
        ss << std::put_time(tm, "%Y-%m-%dT%H:%M:%S");
        return ss.str();
    }

    tcp::socket socket_;
    boost::asio::streambuf buffer_;
    std::unordered_map<std::string, std::ofstream> &logs_;
    std::mutex &logs_mutex_; // Adicionado mutex
};

class Server
{
public:
    Server(boost::asio::io_context &io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port))
    {
        accept();
    }

private:
    void accept()
    {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, tcp::socket socket)
            {
                if (!ec)
                {
                    std::make_shared<Session>(std::move(socket), logs_, logs_mutex_)->start(); // Passar o mutex
                }
                accept();
            });
    }

    tcp::acceptor acceptor_;
    std::unordered_map<std::string, std::ofstream> logs_;
    std::mutex logs_mutex_; // Adicionado mutex
};

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: server <port>\n";
        return 1;
    }

    boost::asio::io_context io_context;
    Server server(io_context, std::atoi(argv[1]));
    io_context.run();

    return 0;
}