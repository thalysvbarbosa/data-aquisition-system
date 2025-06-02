#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <boost/asio.hpp>
#include <ctime>
#include <iomanip>
#include <sstream>

using boost::asio::ip::tcp;

struct LogRecord
{
    char sensor_id[32];
    std::time_t timestamp;
    double value;
};

class Session : public std::enable_shared_from_this<Session>
{
public:
    Session(tcp::socket socket, std::unordered_map<std::string, std::ofstream> &logs)
        : socket_(std::move(socket)), logs_(logs) {}

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
                const std::string &timestamp = parts[2];
                const std::string &value = parts[3];

                LogRecord record;
                std::strncpy(record.sensor_id, sensor_id.c_str(), sizeof(record.sensor_id));
                record.timestamp = string_to_time_t(timestamp);
                record.value = std::stod(value);

                if (logs_.find(sensor_id) == logs_.end())
                {
                    logs_[sensor_id].open(sensor_id + ".log", std::ios::binary | std::ios::app);
                }

                logs_[sensor_id].write(reinterpret_cast<const char *>(&record), sizeof(record));
            }
        }
        else if (message.rfind("GET|", 0) == 0)
        {
            auto parts = split_message(message);
            if (parts.size() == 3)
            {
                const std::string &sensor_id = parts[1];
                int num_records = std::stoi(parts[2]);

                if (logs_.find(sensor_id) != logs_.end())
                {
                    std::ifstream log_file(sensor_id + ".log", std::ios::binary);
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
        ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
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
                    std::make_shared<Session>(std::move(socket), logs_)->start();
                }
                accept();
            });
    }

    tcp::acceptor acceptor_;
    std::unordered_map<std::string, std::ofstream> logs_;
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