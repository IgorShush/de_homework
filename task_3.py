from typing import List


def solution(S, A):
    result = S[0]

    def append_character(temp: str, index: int) -> str:
        if index == 0:
            return temp
        else:
            temp += S[index]
            res = append_character(temp, A[index])
        return res

    return append_character(result, A[0])


print(solution("cdeo", [3, 2, 0, 1])) # code
print(solution("bytdag", [4, 3, 0, 1, 2, 5])) # bat
print(solution("cdeenetpi", [5, 2, 0, 1, 6, 4, 8, 3, 7])) # centipede
