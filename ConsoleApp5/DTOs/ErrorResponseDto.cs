namespace ConsoleApp5.DTOs;

public class ErrorResponseDto
{
    public string Message { get; set; } = string.Empty;

    public ErrorResponseDto() { }

    public ErrorResponseDto(string message)
    {
        Message = message;
    }
}
